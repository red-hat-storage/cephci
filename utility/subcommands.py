import json
import os
import re
import hashlib
import subprocess
from docopt import docopt

# Command-line documentation
DOC = """
Standard script to fetch and process log files from a given URL.

Usage:
    subcommands.py --url <complete_url> --filter <subcomponent_filter>
    subcommands.py (-h | --help)

Options:
    -h --help                      Show this help message
    --url <complete_url>           Complete URL to start fetching logs from
    --filter <subcomponent_filter> Filter logs by subcomponent (e.g., rgw, rbd, rados)
"""

def compute_output_hash(output):
    """Compute a SHA-256 hash for the given output."""
    return hashlib.sha256(json.dumps(output, sort_keys=True).encode("utf-8")).hexdigest()

def clean_log_line(line):
    """Clean and normalize log lines by removing timestamps and redundant characters."""
    line = re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .*? - (DEBUG|INFO) - ", "", line).strip()
    return re.sub(r"\\\|", "", line)

def reconstruct_json(lines):
    """Attempt to reconstruct JSON from log lines."""
    cleaned_lines = [re.sub(r".* - cephci - .*? - (DEBUG|INFO) - ", "", line).strip() for line in lines if line]
    try:
        return json.loads("\n".join(cleaned_lines))
    except json.JSONDecodeError:
        return cleaned_lines

def extract_radosgw_admin_commands(log_lines):
    """Extract radosgw-admin commands and their outputs from log files."""
    results, existing_hashes = [], set()
    current_command, current_output_lines = None, []
    
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
                    results.append({"command": current_command, "output": output, "output_hash": output_hash})
                    existing_hashes.add(output_hash)
            current_command, current_output_lines = cmd_match.group(0).strip(), []
        else:
            current_output_lines.append(cleaned_line)
    
    return {"outputs": results}

def save_to_remote(command, output, subcomponent_filter):
    """Save extracted command outputs directly to the remote path."""
    output_hash = compute_output_hash(output)
    current_dir = os.getcwd()
    url_parts = current_dir.split("/")
    openstack_version = url_parts[6]
    rhel_version = url_parts[7]
    ceph_version_full = url_parts[9]
    subfolder = url_parts[8]
    if "jenkins" in current_dir:
        base_dir = os.path.join("/home/jenkins/magna002/cephci-jenkins/cephci-command-results/",openstack_version,rhel_version,ceph_version_full,subcomponent_filter,subfolder)
    else:
        base_dir = os.path.join(current_dir, "subcommandsoutput", "rgw")

    os.makedirs(base_dir, exist_ok=True)
    
    match = re.search(r"radosgw-admin (\w+)", command)
    if match:
        subcommand = match.group(1)
        file_path = os.path.join(base_dir, f"{subcommand}_outputs.json")
        
        try:
            data = {"outputs": []}
            if os.path.exists(file_path):
                with open(file_path, "r") as file:
                    data = json.load(file)
            
            if not any(entry["output_hash"] == output_hash for entry in data["outputs"]):
                data["outputs"].append({"command": command, "output": output, "output_hash": output_hash})
                with open(file_path, "w") as file:
                    json.dump(data, file, indent=4)
                print(f"Saved output for {subcommand} to {file_path}")
        except Exception as e:
            print(f"Error saving file: {e}")

def get_log_files_from_directory(directory):
    """Retrieve all log file paths from a given directory."""
    return [os.path.join(root, file) for root, _, files in os.walk(directory) for file in files if file.endswith(".log")]

def process_log_file(file_path, subcomponent_filter):
    """Process a log file to extract relevant radosgw-admin commands."""
    try:
        with open(file_path, "r") as file:
            log_lines = file.readlines()
            extracted_data = extract_radosgw_admin_commands(log_lines)
            for entry in extracted_data["outputs"]:
                save_to_remote(entry["command"], entry["output"], subcomponent_filter)
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")

def run(complete_url, subcomponent_filter):
    """Main function to process log files from a given URL."""
    log_files = get_log_files_from_directory(complete_url)
    for file_path in log_files:
        process_log_file(file_path, subcomponent_filter)

if __name__ == "__main__":
    arguments = docopt(DOC)
    run(arguments["--url"], arguments["--filter"])
