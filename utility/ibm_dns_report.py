"""
Utility to generate a CSV report of DNS records from IBM environment
Identifies orphan/stale DNS entries and matches DNS entries with VM names
"""

import csv
import logging
import math
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import yaml
from docopt import docopt

from compute.ibm_vpc import CephVMNodeIBM, get_dns_zone_id

# ---------- Logging ----------
logger = logging.getLogger("ibm_dns_report")
_stream = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
_stream.setFormatter(_formatter)
logger.addHandler(_stream)
logger.setLevel(logging.INFO)
# -----------------------------

doc = """
Utility to generate a CSV report of DNS records from IBM cloud.
    Usage:
        ibm_dns_report.py --creds <cred-file> [--output <csv-file>] [--delete] [--dry-run]
        ibm_dns_report.py (-h | --help)
    Options:
        -h --help          Shows the command usage
        --creds <file>     API Credential file to access ibm cloud.
                           sample/example <file>:
                           globals:
                                ibm-credentials:
                                    access-key: '<user_access_key>'
                                    service-url: 'https://xxxx.iaas.cloud.ibm.com/v1'
                                    zone_name: '<dns_zone>'
                                    vpc_name: '<vpc_name>'
                                    dns-instance-id: '<dns_service_instance_guid>'
        --output <file>    Output CSV file path (default: dns_report_<timestamp>.csv)
        --delete           Delete orphan/stale DNS entries after generating report
        --dry-run          Show what would be deleted without actually deleting
"""

# Hardcoded VM names to exclude from orphan detection (infrastructure VMs)
EXCLUDE_VM_NAMES = {
    "infra-vm-jenkins-01",
    "infra-vm-jenkins-agent-01",
    "infra-vm-jenkins-agent-02",
    "infra-vm-jenkins-agent-03",
    "nginx-vm-01",
    "rgw-vault-server-01",
    "smb-windows-ad-server-vm1",
    "metaltank",
}


def get_all_instances(vm_node: CephVMNodeIBM, vpc_name: str) -> List[Dict]:
    instances = []
    resp = vm_node.service.list_instances(vpc_name=vpc_name)
    if resp.get_status_code() != 200:
        raise Exception("Failed to retrieve instances")

    result = resp.get_result()
    instances.extend(result["instances"])

    if "next" in result:
        start = result["next"]["href"].split("start=")[-1]
        iteration = math.ceil(result["total_count"] / result["limit"])

        for _ in range(1, iteration):
            list_inst = vm_node.service.list_instances(start=start, vpc_name=vpc_name)
            if list_inst.get_status_code() != 200:
                raise Exception("Failed to retrieve instances during pagination")

            list_result = list_inst.get_result()
            instances.extend(list_result["instances"])

            if "next" in list_result:
                start = list_result["next"]["href"].split("start=")[-1]

    if len(instances) != result["total_count"]:
        raise Exception(
            "Failed to list all instances. Total: {total}, Listed: {listed}".format(
                total=result["total_count"], listed=len(instances)
            )
        )

    return instances


def get_all_dns_records(
    vm_node: CephVMNodeIBM, dns_svc_id: str, dns_zone_id: str
) -> List[Dict]:
    all_records = []
    for record in vm_node._list_resource_records_with_pagination(
        dns_svc_id=dns_svc_id, dns_zone_id=dns_zone_id, limit=50
    ):
        all_records.append(record)
    return all_records


def generate_dns_report(
    instances: List[Dict],
    dns_records: List[Dict],
    exclude_vm_names: set = None,
) -> Tuple[List[Dict], List[Dict]]:
    ip_to_vm = {}
    for instance in instances:
        ip = instance["primary_network_interface"]["primary_ip"]["address"]
        vm_name = instance["name"]
        ip_to_vm[ip] = vm_name

    orphan_entries = []
    active_entries = []
    excluded_count = 0

    if exclude_vm_names is None:
        exclude_vm_names = set()

    def is_excluded(dns_name: str) -> bool:
        """Check if the DNS name matches any excluded VM name."""
        # DNS name format is typically: vm-name.zone (e.g., nginx-vm-01.qe.ceph.lab)
        # Extract the hostname part (before the first dot or the full name)
        hostname = dns_name.split(".")[0] if "." in dns_name else dns_name
        return hostname in exclude_vm_names

    for record in dns_records:
        zone_name = record.get("_zone_name", "Unknown")

        if record["type"] == "A":
            dns_name = record["name"]
            dns_ip = record["rdata"]["ip"]
            record_id = record["id"]

            if dns_ip in ip_to_vm:
                active_entries.append(
                    {
                        "status": "Active",
                        "zone_name": zone_name,
                        "record_type": "A",
                        "dns_name": dns_name,
                        "ip_address": dns_ip,
                        "vm_name": ip_to_vm[dns_ip],
                        "record_id": record_id,
                        "has_ptr": "Yes" if record.get("linked_ptr_record") else "No",
                    }
                )
            elif is_excluded(dns_name):
                # Treat excluded VM names as active (protected)
                excluded_count += 1
                active_entries.append(
                    {
                        "status": "Excluded",
                        "zone_name": zone_name,
                        "record_type": "A",
                        "dns_name": dns_name,
                        "ip_address": dns_ip,
                        "vm_name": "EXCLUDED",
                        "record_id": record_id,
                        "has_ptr": "Yes" if record.get("linked_ptr_record") else "No",
                    }
                )
            else:
                orphan_entries.append(
                    {
                        "status": "Orphan/Stale",
                        "zone_name": zone_name,
                        "record_type": "A",
                        "dns_name": dns_name,
                        "ip_address": dns_ip,
                        "vm_name": "N/A",
                        "record_id": record_id,
                        "has_ptr": "Yes" if record.get("linked_ptr_record") else "No",
                    }
                )

        elif record["type"] == "PTR":
            ptr_name = record["name"]
            ptr_target = record["rdata"]["ptrdname"]

            if ptr_name in ip_to_vm:
                active_entries.append(
                    {
                        "status": "Active",
                        "zone_name": zone_name,
                        "record_type": "PTR",
                        "dns_name": ptr_target,
                        "ip_address": ptr_name,
                        "vm_name": ip_to_vm[ptr_name],
                        "record_id": record["id"],
                        "has_ptr": "N/A",
                    }
                )
            elif is_excluded(ptr_target):
                # Treat excluded VM names as active (protected)
                excluded_count += 1
                active_entries.append(
                    {
                        "status": "Excluded",
                        "zone_name": zone_name,
                        "record_type": "PTR",
                        "dns_name": ptr_target,
                        "ip_address": ptr_name,
                        "vm_name": "EXCLUDED",
                        "record_id": record["id"],
                        "has_ptr": "N/A",
                    }
                )
            else:
                orphan_entries.append(
                    {
                        "status": "Orphan/Stale",
                        "zone_name": zone_name,
                        "record_type": "PTR",
                        "dns_name": ptr_target,
                        "ip_address": ptr_name,
                        "vm_name": "N/A",
                        "record_id": record["id"],
                        "has_ptr": "N/A",
                    }
                )

    if excluded_count > 0:
        logger.info("Excluded %s DNS records from orphan detection", excluded_count)

    return orphan_entries, active_entries


def write_csv_report(
    output_file: str, orphan_entries: List[Dict], active_entries: List[Dict]
) -> None:
    fieldnames = [
        "status",
        "zone_name",
        "record_type",
        "dns_name",
        "ip_address",
        "vm_name",
        "record_id",
        "has_ptr",
    ]

    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for entry in orphan_entries:
            writer.writerow(entry)

        for entry in active_entries:
            writer.writerow(entry)

    logger.info("CSV report generated: %s", output_file)
    logger.info("Total orphan/stale entries: %s", len(orphan_entries))
    logger.info("Total active entries: %s", len(active_entries))
    logger.info(
        "Total DNS records analyzed: %s", len(orphan_entries) + len(active_entries)
    )


def delete_orphan_dns_entries(
    vm_node: CephVMNodeIBM,
    orphan_entries: List[Dict],
    instance_id: str,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    NOTE: instance_id is now passed explicitly.
    """
    if not orphan_entries:
        logger.info("No orphan entries to delete.")
        return 0, 0

    if dry_run:
        logger.info("DRY RUN: Deleting %s orphan DNS entries...", len(orphan_entries))
    else:
        logger.info("Deleting %s orphan DNS entries...", len(orphan_entries))

    successful = 0
    failed = 0

    entries_by_zone = {}
    for entry in orphan_entries:
        zone_name = entry.get("zone_name", "Unknown")
        if zone_name not in entries_by_zone:
            entries_by_zone[zone_name] = []
        entries_by_zone[zone_name].append(entry)

    for zone_name, zone_entries in entries_by_zone.items():
        logger.info("Processing zone: %s (%s entries)", zone_name, len(zone_entries))

        zone_id = zone_entries[0].get("_zone_id")
        if not zone_id:
            try:
                dns_zones_response = vm_node.dns_service.list_dnszones(instance_id)
                if dns_zones_response.get_status_code() == 200:
                    zone_id = get_dns_zone_id(
                        zone_name, dns_zones_response.get_result()
                    )
                else:
                    logger.warning(
                        "Could not retrieve zone_id for %s, skipping...", zone_name
                    )
                    failed += len(zone_entries)
                    continue
            except Exception as e:
                logger.error("Error retrieving zone_id for %s: %s", zone_name, e)
                failed += len(zone_entries)
                continue

        for entry in zone_entries:
            record_type = entry["record_type"]
            dns_name = entry["dns_name"]
            record_id = entry["record_id"]
            ip_address = entry["ip_address"]

            try:
                if dry_run:
                    action = "Would delete"
                else:
                    action = "Deleting"

                logger.info(
                    "%s %s record: %s (%s)", action, record_type, dns_name, ip_address
                )

                if not dry_run:
                    if entry.get("has_ptr") == "Yes" and record_type == "A":
                        logger.info("Note: This A record has a linked PTR record")

                    response = vm_node.dns_service.delete_resource_record(
                        instance_id=instance_id,
                        dnszone_id=zone_id,
                        record_id=record_id,
                    )

                    if response.get_status_code() in [204, 200]:
                        successful += 1
                    else:
                        logger.error(
                            "Failed deletion (status %s)",
                            response.get_status_code(),
                        )
                        failed += 1
                else:
                    successful += 1

            except Exception as e:
                logger.error("Error deleting record %s: %s", dns_name, e)
                failed += 1

    if dry_run:
        logger.info("DRY RUN Summary:")
        logger.info("  Successfully would delete: %s", successful)
    else:
        logger.info("Summary:")
        logger.info("  Successfully deleted: %s", successful)

    if failed > 0:
        logger.error("  Failed: %s", failed)

    return successful, failed


def run(args: Dict) -> int:
    cred_file = args["--creds"]
    output_file = args.get("--output")

    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = "dns_report_{ts}.csv".format(ts=timestamp)

    try:
        with open(cred_file, "r") as cred_stream:
            yh = yaml.safe_load(cred_stream)
            ibm_cred = yh["globals"]["ibm-credentials"]

            # ---- instance_id loaded from creds (no globals) ----
            instance_id = ibm_cred["dns_svc_id"]

            os_cred_ibm = {
                "accesskey": ibm_cred["access-key"],
                "service_url": ibm_cred.get(
                    "service-url", "https://eu-de.iaas.cloud.ibm.com/v1"
                ),
            }

            vm_node = CephVMNodeIBM(os_cred_ibm=os_cred_ibm)

            logger.info("Retrieving DNS zones...")
            dns_zones_response = vm_node.dns_service.list_dnszones(instance_id)
            if dns_zones_response.get_status_code() != 200:
                logger.error("Failed to get DNS zones for instance id: %s", instance_id)
                return 1

            dns_zones_result = dns_zones_response.get_result()
            dns_zones = dns_zones_result.get("dnszones", [])
            logger.info("Found %s DNS zone(s)", len(dns_zones))

            all_dns_records = []
            for zone in dns_zones:
                zone_name = zone["name"]
                zone_id = zone["id"]
                zone_state = zone["state"]

                logger.info(
                    "Processing DNS zone: %s (State: %s)...", zone_name, zone_state
                )

                if zone_state != "ACTIVE":
                    logger.info("  Skipping zone %s - not in ACTIVE state", zone_name)
                    continue

                try:
                    zone_records = get_all_dns_records(vm_node, instance_id, zone_id)
                    logger.info(
                        "  Found %s DNS records in %s", len(zone_records), zone_name
                    )

                    for record in zone_records:
                        record["_zone_name"] = zone_name
                        record["_zone_id"] = zone_id

                    all_dns_records.extend(zone_records)
                except Exception as e:
                    logger.warning(
                        "  Could not retrieve DNS records from zone %s: %s",
                        zone_name,
                        e,
                    )
                    continue

            logger.info("Total DNS records across all zones: %s", len(all_dns_records))

            vpc_list_response = vm_node.service.list_vpcs()
            if vpc_list_response.get_status_code() != 200:
                logger.error("Failed to retrieve VPCs")
                return 1

            vpcs = vpc_list_response.get_result().get("vpcs", [])
            all_instances = []

            for vpc in vpcs:
                vpc_name = vpc["name"]
                logger.info("Retrieving VM instances from VPC: %s...", vpc_name)
                try:
                    vpc_instances = get_all_instances(vm_node, vpc_name)
                    logger.info(
                        "Found %s VM instances in %s",
                        len(vpc_instances),
                        vpc_name,
                    )
                    all_instances.extend(vpc_instances)
                except Exception as e:
                    logger.warning(
                        "Could not retrieve instances from VPC %s: %s", vpc_name, e
                    )
                    continue

            logger.info("Total VM instances across all VPCs: %s", len(all_instances))

            logger.info(
                "Excluding %s infrastructure VMs from orphan detection",
                len(EXCLUDE_VM_NAMES),
            )

            logger.info("Analyzing DNS records...")
            orphan_entries, active_entries = generate_dns_report(
                all_instances, all_dns_records, exclude_vm_names=EXCLUDE_VM_NAMES
            )

            logger.info("Writing CSV report...")
            write_csv_report(output_file, orphan_entries, active_entries)

            should_delete = args.get("--delete", False)
            dry_run = args.get("--dry-run", False)

            if should_delete or dry_run:
                for entry in orphan_entries:
                    for record in all_dns_records:
                        if record.get("id") == entry.get("record_id"):
                            entry["_zone_id"] = record.get("_zone_id")
                            break

                successful, failed = delete_orphan_dns_entries(
                    vm_node, orphan_entries, instance_id=instance_id, dry_run=dry_run
                )

                if (not dry_run) and failed > 0:
                    logger.warning(
                        "%s deletions failed. Check the output above.", failed
                    )

            logger.info("Successfully completed DNS report processing")
            return 0

    except FileNotFoundError:
        logger.error("Error: Credential file not found: %s", cred_file)
        return 1
    except KeyError as e:
        logger.error("Error: Missing required configuration key: %s", e)
        return 1
    except Exception:
        logger.exception("Error generating DNS report")
        return 1


if __name__ == "__main__":
    try:
        arguments = docopt(doc)
        rc = run(arguments)
    except Exception:
        logger.exception("Failed to generate DNS report")
        rc = 1
    sys.exit(rc)
