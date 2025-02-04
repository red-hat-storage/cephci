import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import os
import re
import hashlib
from docopt import docopt

doc = """
Standard script to fetch and process log files from a given URL.

Usage:
    upload.py --url <complete_url> --filter <subcomponent_filter>
    upload.py (-h | --help)

Options:
    -h --help                  Shows the command usage
    --url <complete_url>       Complete URL to start fetching logs from.
    --filter <subcomponent_filter>  Filter logs by subcomponent (e.g., rgw, rbd, rados).
"""

global_output_hashes = set()
log_links_dict = {}


def compute_output_hash(output):
    return hashlib.sha256(
        json.dumps(output, sort_keys=True).encode("utf-8")
    ).hexdigest()


def clean_log_line(line):
    timestamp_pattern = (
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .*? - (DEBUG|INFO) - "
    )
    line = re.sub(timestamp_pattern, "", line).strip()
    line = re.sub(r"\\\|", "", line)
    line = re.sub(r"^/ ", "", line)
    return line


def reconstruct_json(lines):
    cleaned_lines = []

    for line in lines:
        line = re.sub(r".* - cephci - .*? - (DEBUG|INFO) - ", "", line).strip()
        if line and not re.match(r"^\d{4}-\d{2}-\d{2}", line):
            cleaned_lines.append(line)

    json_str = "\n".join(cleaned_lines).strip()

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return cleaned_lines


def extract_radosgw_admin_commands(log_lines):
    results = []
    existing_hashes = set()
    current_command, current_output_lines = None, []
    collecting_output = False

    for line in log_lines:
        cleaned_line = clean_log_line(line)
        if not cleaned_line:
            continue

        cmd_match = re.search(r"(?:sudo )?radosgw-admin[^;\n]+", cleaned_line)
        if cmd_match:
            if current_command and current_output_lines:
                output = reconstruct_json(current_output_lines)
                output_hash = compute_output_hash(output)
                if output_hash not in existing_hashes:
                    results.append(
                        {
                            "command": current_command,
                            "output": output,
                            "output_hash": output_hash,
                        }
                    )
                    existing_hashes.add(output_hash)

            current_command = cmd_match.group(0).strip()
            current_output_lines = []
            collecting_output = True

        elif collecting_output:
            if "executing cmd:" in cleaned_line:
                collecting_output = False
            else:
                current_output_lines.append(cleaned_line)

    if current_command and current_output_lines:
        output = reconstruct_json(current_output_lines)
        output_hash = compute_output_hash(output)
        if output_hash not in existing_hashes:
            results.append(
                {
                    "command": current_command,
                    "output": output,
                    "output_hash": output_hash,
                }
            )
            existing_hashes.add(output_hash)

    return {"outputs": results}


def fetch_log_links(url, base_url=None, allow=True):
    if base_url is None:
        base_url = url
    if url not in log_links_dict:
        log_links_dict[url] = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            absolute_url = urljoin(url, href)
            if href.endswith(".log"):
                log_links_dict[url].append({"opt_in": absolute_url})
            elif href.endswith("/") and allow:
                fetch_log_links(absolute_url, base_url, False)
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")


def process_log_file(file_url, complete_url, remote_base_dir, subcomponent_filter):
    try:
        response = requests.get(file_url)
        response.raise_for_status()
        log_lines = response.text.splitlines()
        extracted_data = extract_radosgw_admin_commands(log_lines)
        for entry in extracted_data["outputs"]:
            save_to_json(
                entry["command"],
                entry["output"],
                complete_url,
                remote_base_dir,
                subcomponent_filter,
            )
    except requests.RequestException as e:
        print(f"Failed to download {file_url}: {e}")


def process_all_log_files(url, remote_base_dir, subcomponent_filter):
    fetch_log_links(url)
    for directories in log_links_dict:
        for log_file in log_links_dict[directories]:
            process_log_file(
                log_file["opt_in"], url, remote_base_dir, subcomponent_filter
            )


def save_to_json(command, output, complete_url, remote_base_dir, subcomponent_filter):
    global global_output_hashes
    output_hash = compute_output_hash(output)

    if output_hash in global_output_hashes:
        return
    global_output_hashes.add(output_hash)

    current_dir = os.getcwd()

    if "jenkins" in current_dir:
        url_parts = complete_url.strip("/").split("/")
        # Correct indices for transformed URL structure
        openstack_version = url_parts[7]  # RH
        rhel_version = url_parts[8]  # 8.0
        ceph_version = url_parts[10]  # 19.2.0-73
        subfolder = url_parts[9]  # Test

        remote_dir = os.path.join(
            remote_base_dir,
            openstack_version,
            rhel_version,
            ceph_version,
            subcomponent_filter,
            subfolder,
        )
        os.makedirs(remote_dir, exist_ok=True)

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            remote_file_path = os.path.join(remote_dir, f"{subcommand}_outputs.json")

            if os.path.exists(remote_file_path):
                with open(remote_file_path, "r") as remote_file:
                    try:
                        data = json.load(remote_file)
                    except json.JSONDecodeError:
                        data = {"ceph_version": ceph_version, "outputs": []}
            else:
                data = {"ceph_version": ceph_version, "outputs": []}

            if not any(
                entry["output_hash"] == output_hash for entry in data["outputs"]
            ):
                data["outputs"].append(
                    {
                        "command": command,
                        "output": output,
                        "output_hash": output_hash,
                    }
                )
                with open(remote_file_path, "w") as remote_file:
                    json.dump(data, remote_file, indent=4)

    else:
        local_dir = "/Users/suriya/Desktop/subcommandsoutput/output11"
        os.makedirs(local_dir, exist_ok=True)

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            local_file_path = os.path.join(local_dir, f"{subcommand}_outputs.json")

            if os.path.exists(local_file_path):
                with open(local_file_path, "r") as local_file:
                    try:
                        data = json.load(local_file)
                    except json.JSONDecodeError:
                        data = {"outputs": []}
            else:
                data = {"outputs": []}

            if not any(
                entry["output_hash"] == output_hash for entry in data["outputs"]
            ):
                data["outputs"].append(
                    {
                        "command": command,
                        "output": output,
                        "output_hash": output_hash,
                    }
                )
                with open(local_file_path, "w") as local_file:
                    json.dump(data, local_file, indent=4)


def run(complete_url: str, subcomponent_filter: str):
    remote_base_dir = (
        "http://magna002.ceph.redhat.com/cephci-jenkins/cephci-command-results/"
    )
    process_all_log_files(complete_url, remote_base_dir, subcomponent_filter)


if __name__ == "__main__":
    arguments = docopt(doc)
    complete_url = arguments["--url"]
    subcomponent_filter = arguments["--filter"]
    run(complete_url, subcomponent_filter)
