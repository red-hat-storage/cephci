"""
This script is designed to process local log files, extract specific command outputs, and save them in structured JSON format. It ensures efficient log parsing, prevents duplicate storage, and organizes outputs based on subcomponents.

### **Features:**
1. **Recursive Log Retrieval:**  
   - The script scans the specified directory to find `.log` files for processing.  

2. **Command Extraction:**  
   - Identifies and extracts `radosgw-admin` commands from log files.  
   - Parses corresponding command outputs and structures them in JSON format.  

3. **Output Storage:**  
   - Saves extracted data into organized JSON files based on subcommands.  
   - Ensures no duplicate entries using hash-based validation.  

4. **Automatic Directory & File Handling:**  
   - Creates necessary directories and JSON files if they don’t exist.  
   - Organizes extracted outputs by subcomponent filters.  

5. **Duplicate Detection:**  
   - Computes SHA-256 hashes for command outputs to prevent redundant storage.  

6. **Error Handling:**  
   - Manages file I/O operations and JSON parsing errors gracefully.  

### **How the Script Works:**
1. **Fetching Log Files:**  
   - The `get_log_files_from_directory` function scans the given directory and collects all `.log` files.  

2. **Processing Log Files:**  
   - The `process_log_file` function reads each `.log` file, extracts `radosgw-admin` commands, and processes outputs.  

3. **Command Extraction & Deduplication:**  
   - The `extract_radosgw_admin_commands` function:  
     - Identifies `radosgw-admin` commands within log lines.  
     - Extracts and reconstructs command outputs.  
     - Uses SHA-256 hashes to detect and prevent duplicate entries.  

4. **Saving Outputs:**  
   - The `save_to_remote` function:  
     - Creates the required directory structure.  
     - Saves extracted command outputs into JSON files categorized by subcommands.  
     - Checks for duplicates before appending new entries.  

5. **Execution Flow:**  
   - The `run` function:  
     - Retrieves `.log` files from the specified directory.  
     - Processes each file, extracting relevant commands and saving outputs.  

### **Key Functions:**
- `get_log_files_from_directory(directory)`: Retrieves all `.log` files from a directory.  
- `process_log_file(file_path, subcomponent_filter, output_directory)`: Processes each log file for command extraction.  
- `extract_radosgw_admin_commands(log_lines)`: Extracts `radosgw-admin` commands and reconstructs outputs.  
- `compute_output_hash(output)`: Computes a unique SHA-256 hash for deduplication.  
- `save_to_remote(command, output, subcomponent_filter, output_directory)`: Saves extracted command data into structured JSON files.  
- `run(log_directory, subcomponent_filter, output_directory)`: Orchestrates log file processing and output storage.  

### **Prerequisites:**
1. Install the following Python libraries:  
   - `json`  
   - `os`  
   - `re`  
   - `hashlib`  
   - `docopt`  

2. Ensure the log directory is accessible and contains `.log` files.  

---

### **Folder Structure:**
The script organizes extracted outputs into the specified output directory as follows:  

"""
import json
import os
import re
import hashlib
from docopt import docopt

DOC = """
Standard script to fetch and process log files from a given directory.

Usage:
    local.py --logdir <log_directory> --filter <subcomponent_filter> --outdir <output_directory>
    local.py (-h | --help)

Options:
    -h --help                      Show this help message
    --logdir <log_directory>       Directory containing log files
    --filter <subcomponent_filter> Filter logs by subcomponent (e.g., rgw, rbd, rados)
    --outdir <output_directory>    Directory where output JSON files will be stored
"""

def compute_output_hash(output):
    return hashlib.sha256(json.dumps(output, sort_keys=True).encode("utf-8")).hexdigest()

def clean_log_line(line):
 
    line = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - cephci - ceph:\d+ - (INFO|DEBUG|ERROR) -\s*", "", line).strip()
    line = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (INFO|DEBUG|ERROR):?\s*", "", line).strip()
    return line if line else None

def reconstruct_json(lines):

    cleaned_lines = [clean_log_line(line) for line in lines if clean_log_line(line)]
    try:
        return json.loads("\n".join(cleaned_lines))
    except json.JSONDecodeError:
        return cleaned_lines

def extract_radosgw_admin_commands(log_lines):
  
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

def save_to_remote(command, output, subcomponent_filter, output_directory):
    """Save extracted command outputs to the specified output directory."""
    output_hash = compute_output_hash(output)
    base_dir = os.path.join(output_directory, subcomponent_filter)

    # Ensure the directory exists
    os.makedirs(base_dir, exist_ok=True)

    subcommand_match = re.search(r"radosgw-admin (\w+)", command)
    if subcommand_match:
        subcommand = subcommand_match.group(1)
        file_path = os.path.join(base_dir, f"{subcommand}_outputs.json")

        try:
            # Ensure the file exists and initialize if necessary
            if not os.path.exists(file_path):
                with open(file_path, "w") as file:
                    json.dump({"outputs": []}, file)

            # Read existing data
            with open(file_path, "r") as file:
                data = json.load(file)

            # Append new entry if it doesn't already exist
            if not any(entry["output_hash"] == output_hash for entry in data["outputs"]):
                data["outputs"].append({"command": command, "output": output, "output_hash": output_hash})

                # Write back to file
                with open(file_path, "w") as file:
                    json.dump(data, file, indent=4)

                print(f"Saved output for {subcommand} to {file_path}")

        except Exception as e:
            print(f"Error saving file: {e}")
            
def get_log_files_from_directory(directory):

    if not isinstance(directory, str):
        raise TypeError("Expected a string path for directory")
    return [os.path.join(root, file) for root, _, files in os.walk(directory) for file in files if file.endswith(".log")]

def process_log_file(file_path, subcomponent_filter, output_directory):

    try:
        with open(file_path, "r") as file:
            log_lines = file.readlines()
            extracted_data = extract_radosgw_admin_commands(log_lines)
            for entry in extracted_data["outputs"]:
                save_to_remote(entry["command"], entry["output"], subcomponent_filter, output_directory)
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")

def run(log_directory, subcomponent_filter, output_directory):
 
    if not isinstance(log_directory, str) or not os.path.isdir(log_directory):
        raise ValueError("Invalid log directory provided.")
    if not isinstance(output_directory, str):
        raise ValueError("Invalid output directory provided.")
    
    log_files = get_log_files_from_directory(log_directory)
    for file_path in log_files:
        process_log_file(file_path, subcomponent_filter, output_directory)

if __name__ == "__main__":
    arguments = docopt(DOC)
    run(arguments["--logdir"], arguments["--filter"], arguments["--outdir"])
