import json
import os
import re
import hashlib
import subprocess
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
    """Extracts radosgw-admin commands and their output from logs."""
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


def copy_to_remote(local_path, subcomponent_filter):
    """Copies output files to the remote path using subprocess.Popen."""
    parts = local_path.split("/")
    try:
        index_rh = parts.index("RH")
        structured_parts = [
            parts[index_rh + 1],  # 8.0
            parts[index_rh + 2],  # rhel-9.5
            parts[index_rh + 4],  # 19.2.0-73
            subcomponent_filter,  # Dynamic part based on argument
            parts[index_rh + 3],  # Test
        ]
        remote_path = os.path.join(
            "/home/jenkins/magna002/cephci-jenkins/cephci-command-results/", *structured_parts
        )
        
        print(f"Copying files from {local_path} to {remote_path}...")

        os.makedirs(remote_path, exist_ok=True)
        subprocess.run(["sudo", "cp", "-r", local_path, remote_path], check=True)
        print("Successfully copied files to the remote path!")

    except Exception as e:
        print(f"Error copying files to remote path: {e}")


def save_to_json(command, output, complete_url, subcomponent_filter):
    """Saves extracted command output to a structured JSON file."""
    global global_output_hashes
    output_hash = compute_output_hash(output)

    if output_hash in global_output_hashes:
        return
    global_output_hashes.add(output_hash)

    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")
    
    if "jenkins" in current_dir:
        local_base_dir = os.path.join(current_dir, subcomponent_filter, "outputs")
        target_path = os.path.join(local_base_dir, subcomponent_filter)
        os.makedirs(target_path, exist_ok=True)

        match = re.search(r"radosgw-admin (\w+)", command)
        if match:
            subcommand = match.group(1)
            local_file_path = os.path.join(target_path, f"{subcommand}_outputs.json")

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

        # Copy to the remote path
        copy_to_remote(local_base_dir, subcomponent_filter)


def get_log_files_from_directory(directory):
    """Returns a list of all .log files in a given directory."""
    log_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".log"):
                log_files.append(os.path.join(root, file))
    return log_files


def process_log_file(file_path, complete_url, subcomponent_filter):
    """Processes a log file to extract and store command outputs."""
    try:
        with open(file_path, "r") as file:
            log_lines = file.readlines()
            extracted_data = extract_radosgw_admin_commands(log_lines)
            for entry in extracted_data["outputs"]:
                save_to_json(entry["command"], entry["output"], complete_url, subcomponent_filter)
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")


def run(complete_url: str, subcomponent_filter: str):
    """Main function to process all log files in a given directory."""
    log_files = get_log_files_from_directory(complete_url)
    for file_path in log_files:
        process_log_file(file_path, complete_url, subcomponent_filter)


if __name__ == "__main__":
    arguments = docopt(doc)
    complete_url = arguments["--url"]
    subcomponent_filter = arguments["--filter"]

    run(complete_url, subcomponent_filter)
