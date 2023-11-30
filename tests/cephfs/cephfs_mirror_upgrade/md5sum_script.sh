#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <folder_path>"
    exit 1
fi

folder_path="$1"
shopt -s nullglob globstar

for file in "$folder_path"/{*,**/*}; do
    if [[ -f "$file" ]]; then
        md5sum "$file" | awk -v folder_path="$folder_path" '{gsub(folder_path, ""); print}'
    fi
done
