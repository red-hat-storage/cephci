"""Generate a mixed dataset of small + large files for CephFS mirroring perf tests.

Usage:
    python generate_mixed_dataset.py <base_dir> \
        <num_small_files> <min_kb> <max_kb> <num_small_dirs> \
        <num_large_files> <large_file_size_mb> <num_large_dirs>

Small files are spread evenly across <num_small_dirs> subdirectories
(small_dir_001 .. small_dir_NNN), each with random sizes between
<min_kb> and <max_kb> KiB, content from /dev/urandom.

Large files are spread evenly across <num_large_dirs> subdirectories
(large_dir_01 .. large_dir_NN), each exactly <large_file_size_mb> MiB,
written in 1 MiB chunks from /dev/urandom.
"""

import math
import os
import random
import sys

if len(sys.argv) != 9:
    print(
        "Usage: python generate_mixed_dataset.py <base_dir> "
        "<num_small_files> <min_kb> <max_kb> <num_small_dirs> "
        "<num_large_files> <large_file_size_mb> <num_large_dirs>"
    )
    sys.exit(1)

base_dir = sys.argv[1]
num_small_files = int(sys.argv[2])
min_kb = int(sys.argv[3])
max_kb = int(sys.argv[4])
num_small_dirs = int(sys.argv[5])
num_large_files = int(sys.argv[6])
large_file_size_mb = int(sys.argv[7])
num_large_dirs = int(sys.argv[8])

CHUNK = 4096
MB = 1024 * 1024

# ---- Phase 1: Small files across directories ----
files_per_small_dir = math.ceil(num_small_files / num_small_dirs)
small_total_bytes = 0
small_count = 0

print(
    f"Phase 1: {num_small_files} small files ({min_kb}-{max_kb} KB) "
    f"across {num_small_dirs} directories"
)

with open("/dev/urandom", "rb") as urandom:
    for d in range(1, num_small_dirs + 1):
        dir_path = os.path.join(base_dir, f"small_dir_{d:03d}")
        os.makedirs(dir_path, exist_ok=True)

        start = small_count
        end = min(start + files_per_small_dir, num_small_files)

        for i in range(start, end):
            size = random.randint(min_kb, max_kb) * 1024
            filepath = os.path.join(dir_path, f"sfile_{i + 1:06d}.dat")
            with open(filepath, "wb") as f:
                remaining = size
                while remaining > 0:
                    chunk = urandom.read(min(CHUNK, remaining))
                    f.write(chunk)
                    remaining -= len(chunk)
            small_total_bytes += size
            small_count += 1

        if d % 10 == 0 or d == num_small_dirs:
            print(
                f"  Small dirs: {d}/{num_small_dirs} done, "
                f"{small_count} files, {small_total_bytes / MB:.1f} MB"
            )

        if small_count >= num_small_files:
            break

print(
    f"Phase 1 done: {small_count} small files, "
    f"{small_total_bytes / MB:.1f} MB across {min(d, num_small_dirs)} dirs"
)

# ---- Phase 2: Large files across directories ----
files_per_large_dir = math.ceil(num_large_files / num_large_dirs)
large_total_bytes = 0
large_count = 0
chunk_1mb = MB

print(
    f"Phase 2: {num_large_files} large files ({large_file_size_mb} MB each) "
    f"across {num_large_dirs} directories"
)

with open("/dev/urandom", "rb") as urandom:
    for d in range(1, num_large_dirs + 1):
        dir_path = os.path.join(base_dir, f"large_dir_{d:02d}")
        os.makedirs(dir_path, exist_ok=True)

        start = large_count
        end = min(start + files_per_large_dir, num_large_files)

        for i in range(start, end):
            filepath = os.path.join(dir_path, f"lfile_{i + 1:04d}.dat")
            file_bytes = large_file_size_mb * MB
            with open(filepath, "wb") as f:
                remaining = file_bytes
                while remaining > 0:
                    chunk = urandom.read(min(chunk_1mb, remaining))
                    f.write(chunk)
                    remaining -= len(chunk)
            large_total_bytes += file_bytes
            large_count += 1
            print(
                f"  Large file {large_count}/{num_large_files}: "
                f"{filepath} ({large_file_size_mb} MB)"
            )

        if large_count >= num_large_files:
            break

print(
    f"Phase 2 done: {large_count} large files, "
    f"{large_total_bytes / MB:.1f} MB across {min(d, num_large_dirs)} dirs"
)

grand_total = small_total_bytes + large_total_bytes
total_files = small_count + large_count
print(
    f"\nDataset complete: {total_files} files, "
    f"{grand_total / (1024 * MB):.2f} GiB total in {base_dir}"
)
