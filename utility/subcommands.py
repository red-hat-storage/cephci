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


import os

def save_to_json(command, output, complete_url, subcomponent_filter):
    global global_output_hashes
    output_hash = compute_output_hash(output)

    if output_hash in global_output_hashes:
        return
    global_output_hashes.add(output_hash)

    current_dir = os.getcwd()
    print("Current working directory:", current_dir)

    if "jenkins" in current_dir:
        remote_base_dir = "/home/jenkins/cephci-jenkins/cephci-command-results/"
        url_parts = complete_url.strip("/").split("/")
        openstack_version, rhel_version, subfolder, ceph_version = url_parts[5:9]

        remote_dir = os.path.join(
            remote_base_dir, openstack_version, rhel_version, ceph_version, subcomponent_filter, subfolder
        )
        
        print(f"Trying to save to remote directory: {remote_dir}")
        
        # Check if the directory exists, and if not, create it
        if not os.path.exists(remote_dir):
            print(f"Directory does not exist, creating: {remote_dir}")
            try:
                os.makedirs(remote_dir, exist_ok=True)
            except Exception as e:
                print(f"Failed to create remote directory: {e}")
                return  # Stop here if we can't create the directory

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            remote_file_path = os.path.join(remote_dir, f"{subcommand}_outputs.json")

            print(f"Attempting to save file: {remote_file_path}")

            try:
                if os.path.exists(remote_file_path):
                    with open(remote_file_path, "r") as remote_file:
                        try:
                            data = json.load(remote_file)
                        except json.JSONDecodeError:
                            data = {"outputs": []}
                else:
                    data = {"outputs": []}

                if not any(entry["output_hash"] == output_hash for entry in data["outputs"]):
                    data["outputs"].append({"command": command, "output": output, "output_hash": output_hash})
                    with open(remote_file_path, "w") as remote_file:
                        json.dump(data, remote_file, indent=4)

                    print(f"Saved output for {subcommand} to remote directory: {remote_file_path}")
                else:
                    print(f"Duplicate output detected. Not saving {subcommand}_outputs.json")

            except Exception as e:
                print(f"Error saving file: {e}")

def get_log_files_from_directory(directory):
    log_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.log'):
                log_files.append(os.path.join(root, file))
    return log_files


def process_log_file(file_path, complete_url, subcomponent_filter):
    try:
        with open(file_path, "r") as file:
            log_lines = file.readlines()
            extracted_data = extract_radosgw_admin_commands(log_lines)
            for entry in extracted_data["outputs"]:
                save_to_json(entry["command"], entry["output"], complete_url, subcomponent_filter)
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")


def run(complete_url: str, subcomponent_filter: str):
    # Get all log files from the provided directory
    log_files = get_log_files_from_directory(complete_url)
    for file_path in log_files:
        process_log_file(file_path, complete_url, subcomponent_filter)


if __name__ == "__main__":
    arguments = docopt(doc)
    complete_url = arguments["--url"]
    subcomponent_filter = arguments["--filter"]

    run(complete_url, subcomponent_filter)
