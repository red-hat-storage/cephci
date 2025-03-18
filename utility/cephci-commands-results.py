import hashlib
import json
import os
import re
from collections import defaultdict
from docopt import docopt

DOC = """
Standard script to fetch and process log files from a given directory.

Usage:
    subcommands.py --logdir <log_directory> --filter <subcomponent_filter> --outdir <output_directory>
    subcommands.py (-h | --help)

Options:
    -h --help                      Show this help message
    --logdir <log_directory>       Directory containing log files
    --filter <subcomponent_filter> Filter logs by subcomponent (e.g., rgw, rbd, rados)
    --outdir <output_directory>    Directory where output JSON files will be stored
"""


def compute_output_hash(output):
    """Compute SHA-256 hash for output data."""
    return hashlib.sha256(json.dumps(output, sort_keys=True).encode()).hexdigest()


def clean_log_line(line):
    """Clean log line from timestamps and prefixes."""
    return re.sub(
        r"^\d{4}-\d{2}-\d{2}.*?(INFO|DEBUG|ERROR)[:\s-]*", 
        "", 
        line
    ).strip() or None


def reconstruct_json(lines):
    """Try to parse cleaned lines as JSON or return raw lines."""
    try:
        return json.loads("\n".join(lines))
    except json.JSONDecodeError:
        return lines

def load_existing_hashes(output_dir):
    """Preload existing hashes from output JSON files."""
    hashes = set()
    base_dir = os.path.join(output_dir, "cephci-commands-results")
    if not os.path.exists(base_dir):
        return hashes
        
    for fname in os.listdir(base_dir):
        if fname.endswith("_outputs.json"):
            try:
                with open(os.path.join(base_dir, fname), "r") as f:
                    data = json.load(f)
                    hashes.update(e["output_hash"] for e in data.get("outputs", []))
            except Exception:
                pass
    return hashes

def process_log_file(file_path, existing_hashes):
    """Streaming log processor yielding new command outputs."""
    current_command, output_lines = None, []
    
    with open(file_path, "r") as f:
        for line in f:
            cleaned = clean_log_line(line)
            if not cleaned:
                continue

            if match := re.search(r"(radosgw-admin\s+[\w-]+)", cleaned):
                if current_command:
                    output = reconstruct_json(output_lines)
                    output_hash = compute_output_hash(output)
                    if output_hash not in existing_hashes:
                        existing_hashes.add(output_hash)
                        subcommand_match = re.match(r"radosgw-admin\s+([\w-]+)", current_command)
                        if subcommand_match:
                            yield {
                                "command": current_command,
                                "output": output,
                                "hash": output_hash,
                                "subcommand": subcommand_match.group(1)
                            }
                
                current_command = match.group(0).strip()
                output_lines = []
            else:
                output_lines.append(cleaned)

        if current_command and output_lines:
            output = reconstruct_json(output_lines)
            output_hash = compute_output_hash(output)
            if output_hash not in existing_hashes:
                existing_hashes.add(output_hash)
                subcommand_match = re.match(r"radosgw-admin\s+([\w-]+)", current_command)
                if subcommand_match:
                    yield {
                        "command": current_command,
                        "output": output,
                        "hash": output_hash,
                        "subcommand": subcommand_match.group(1)
                    }


def run(log_dir, _, output_dir):
    """Main processing workflow with batched writes."""
    existing_hashes = load_existing_hashes(output_dir)
    buffers = defaultdict(list)
    
    for log_file in get_log_files(log_dir):
        for entry in process_log_file(log_file, existing_hashes):
            buffers[entry["subcommand"]].append(entry)
    
    # Batch write all results
    base_dir = os.path.join(output_dir, "cephci-commands-results")
    os.makedirs(base_dir, exist_ok=True)
    
    for subcmd, entries in buffers.items():
        fpath = os.path.join(base_dir, f"{subcmd}_outputs.json")
        existing = []
        
        if os.path.exists(fpath):
            with open(fpath, "r") as f:
                existing = json.load(f).get("outputs", [])
        
        new_entries = [
            {"command": e["command"], "output": e["output"], "output_hash": e["hash"]}
            for e in entries
        ]
        
        with open(fpath, "w") as f:
            json.dump(
                {"outputs": existing + new_entries},
                f,
                indent=2
            )


def get_log_files(directory):
    """Get all .log files recursively from directory."""
    return [
        os.path.join(root, f)
        for root, _, files in os.walk(directory)
        for f in files
        if f.endswith(".log")
    ]


if __name__ == "__main__":
    args = docopt(DOC)
    run(
        args["--logdir"],
        args["--filter"],
        args["--outdir"]
    )