import os
import sys

if len(sys.argv) != 4:
    print(
        "Usage: python generate_multiple_files.py <directory_path> <number_of_files> <size_in_GB>"
    )
    sys.exit(1)

directory_path = sys.argv[1]
number_of_files = int(sys.argv[2])
target_gb = int(sys.argv[3])

# Loop to create multiple files
for file_index in range(1, number_of_files + 1):
    # Generate filename for each file
    filename = os.path.join(
        directory_path, f"generated_file_{target_gb}GB_{file_index}.txt"
    )

    size = 0
    target = target_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    line_number = 1

    with open(filename, "w") as f:
        while size < target:
            line = f"Creating file of {target_gb}GB and this is the line number {line_number}\n"
            f.write(line)
            size += len(line)
            line_number += 1

    print(f"File {file_index} created successfully: {filename} ({target_gb} GB)")

print("All files created successfully!")
