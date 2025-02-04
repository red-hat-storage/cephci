import json
import os
import re
import hashlib
from docopt import docopt

doc = """
Standard script to fetch and process log files from a given URL.

Usage:
    subcommands.py --url <complete_url> --filter <subcomponent_filter>
    subcommands.py (-h | --help)

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
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .*? - (DEBUG|INFO) - "
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
                        {"command": current_command, "output": output, "output_hash": output_hash}
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
            results.append({"command": current_command, "output": output, "output_hash": output_hash})
            existing_hashes.add(output_hash)

    return {"outputs": results}

def ensure_directory_structure(base_path, sub_dirs):
    current_path = base_path
    for sub_dir in sub_dirs:
        current_path = os.path.join(current_path, sub_dir)
        if not os.path.exists(current_path):
            os.makedirs(current_path)
        os.chdir(current_path)


def save_to_json(command, output, complete_url, subcomponent_filter):
    global global_output_hashes
    output_hash = compute_output_hash(output)

    if output_hash in global_output_hashes:
        return
    global_output_hashes.add(output_hash)

    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")

    # If in Jenkins, store remotely
    if "jenkins" in current_dir:
        base_dir = "http://magna002.ceph.redhat.com/cephci-jenkins/cephci-command-results"
        url_parts = complete_url.strip("/").split("/")
        openstack_version, rhel_version, subfolder, ceph_version = url_parts[5:9]

        target_structure = [openstack_version, rhel_version, ceph_version, subcomponent_filter, subfolder]
        ensure_directory_structure(base_dir, target_structure)

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            remote_file_path = f"/home/jenkins/magna002/cephci-jenkins/cephci-command-results/{'/'.join(target_structure)}/{subcommand}_outputs.json"
            print(f"Generated remote file path: {remote_file_path}")
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

            except Exception as e:
                print(f"Error saving file in remote directory: {e}")
    # If local, store in a specific local directory
    else:
        local_dir = "/Users/suriya/Desktop/subcommandsoutput/output30"
        os.makedirs(local_dir, exist_ok=True)

        os.chdir(local_dir)  # Move into the target directory

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            local_file_path = os.path.join(local_dir, f"{subcommand}_outputs.json")

            try:
                if os.path.exists(local_file_path):
                    with open(local_file_path, "r") as local_file:
                        try:
                            data = json.load(local_file)
                        except json.JSONDecodeError:
                            data = {"outputs": []}
                else:
                    data = {"outputs": []}

                if not any(entry["output_hash"] == output_hash for entry in data["outputs"]):
                    data["outputs"].append({"command": command, "output": output, "output_hash": output_hash})
                    with open(local_file_path, "w") as local_file:
                        json.dump(data, local_file, indent=4)

                    print(f"Saved output for {subcommand} to local directory: {local_file_path}")

            except Exception as e:
                print(f"Error saving file in local directory: {e}")


def get_log_files_from_directory(directory):
    log_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".log"):
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
    log_files = get_log_files_from_directory(complete_url)
    for file_path in log_files:
        process_log_file(file_path, complete_url, subcomponent_filter)


if __name__ == "__main__":
    arguments = docopt(doc)
    complete_url = arguments["--url"]
    subcomponent_filter = arguments["--filter"]

    run(complete_url, subcomponent_filter)
