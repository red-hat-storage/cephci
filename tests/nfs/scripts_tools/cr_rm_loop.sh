#!/usr/bin/sh

dir="$1"

if [ -z "$dir" ]; then
 echo "Usage: $0 <directory>"
 exit 1
fi

while true; do
 dd if=/dev/random of="$dir/testfile.txt" bs=1k count=1
 echo "Created file testfile.txt"

 # Listing contents quietly
 ls -lrt "$dir" > /dev/null

 rm -f "$dir/testfile.txt"
 echo "Deleted file testfile.txt"

done
