#!/bin/bash

# Check for correct number of arguments
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <path> <num_of_files> <batch_size>"
  exit 1
fi

# Command-line arguments
path="$1"          # First argument: target directory
num_of_files="$2"  # Second argument: total number of files
batch_size="$3"    # Third argument: batch size

# Validate numeric inputs
if ! [[ "$num_of_files" =~ ^[0-9]+$ ]] || ! [[ "$batch_size" =~ ^[0-9]+$ ]]; then
  echo "Error: num_of_files and batch_size must be positive integers."
  exit 1
fi

# Ensure the target directory exists
sudo mkdir -p "$path"

# Main loop for file creation
for ((i = 0; i < num_of_files; i += batch_size)); do
  batch_end=$((i + batch_size))
  if ((batch_end > num_of_files)); then
    batch_end=$num_of_files
  fi

  for ((j = i; j < batch_end; j++)); do
    file_path="$path/file_$j.txt"
    printf "Created file %d\n" "$j" > "$file_path"
  done
done

echo "File creation completed!"

##Creating file: /mnt/cephfs-func1_fuse0o2fbln9nk_2/file_99998.txt
  #Creating file: /mnt/cephfs-func1_fuse0o2fbln9nk_2/file_99999.txt
  #
  #real	77m25.091s
  #user	11m34.726s
  #sys	16m52.106s
