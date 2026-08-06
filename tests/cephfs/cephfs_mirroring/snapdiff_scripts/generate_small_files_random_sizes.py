"""Generate many small files with random sizes for CephFS mirroring perf tests.

Usage:
    python generate_small_files_random_sizes.py <directory> <num_files> <min_kb> <max_kb>

Creates <num_files> files in <directory>, each with a random size uniformly
distributed between <min_kb> and <max_kb> (in KiB). File content is random
bytes from /dev/urandom read in 4 KiB chunks.
"""

import os
import random
import sys

if len(sys.argv) != 5:
    print(
        "Usage: python generate_small_files_random_sizes.py "
        "<directory> <num_files> <min_kb> <max_kb>"
    )
    sys.exit(1)

directory = sys.argv[1]
num_files = int(sys.argv[2])
min_kb = int(sys.argv[3])
max_kb = int(sys.argv[4])

os.makedirs(directory, exist_ok=True)

CHUNK = 4096
total_bytes = 0

with open("/dev/urandom", "rb") as urandom:
    for i in range(1, num_files + 1):
        size = random.randint(min_kb, max_kb) * 1024
        filepath = os.path.join(directory, f"file_{i:06d}.dat")
        with open(filepath, "wb") as f:
            remaining = size
            while remaining > 0:
                chunk = urandom.read(min(CHUNK, remaining))
                f.write(chunk)
                remaining -= len(chunk)
        total_bytes += size
        if i % 1000 == 0:
            print(
                f"  Created {i}/{num_files} files ({total_bytes / (1024*1024):.1f} MB)"
            )

print(
    f"Done: {num_files} files, {total_bytes / (1024*1024):.1f} MB total "
    f"in {directory}"
)
