import os
import sys

if len(sys.argv) not in [4, 5]:
    print(
        "Usage: python generate_multiple_files.py <directory_path> <number_of_files> <size> [unit: MB|GB]"
    )
    sys.exit(1)

directory_path = sys.argv[1]
number_of_files = int(sys.argv[2])
target_size = int(sys.argv[3])
unit = sys.argv[4].upper() if len(sys.argv) == 5 else "GB"

if unit not in ["GB", "MB"]:
    print("Error: Unit must be either 'GB' or 'MB'")
    sys.exit(1)

# Convert size to bytes
if unit == "GB":
    target_bytes = target_size * 1024 * 1024 * 1024
else:  # MB
    target_bytes = target_size * 1024 * 1024


# Create files
for file_index in range(1, number_of_files + 1):
    filename = os.path.join(
        directory_path, f"generated_file_{target_size}{unit}_{file_index}.txt"
    )

    size = 0
    line_number = 1

    with open(filename, "w") as f:
        while size < target_bytes:
            line = f"Creating file of {target_size}{unit} and this is the line number {line_number}\n"
            f.write(line)
            size += len(line)
            line_number += 1

    print(f"File {file_index} created successfully: {filename} ({target_size} {unit})")

print("All files created successfully!")
