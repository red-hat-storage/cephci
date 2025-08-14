#!/bin/bash

# List of test directories
DIRS=(
   "/mnt/scale/exp1_mt1"
   "/mnt/scale/exp1_mt2"
   "/mnt/scale/exp1_mt3"
)

PIDS=()
HOSTNAME=$(hostname)

# Create logs directory if not exists
mkdir -p logs

# Cleanup on exit
cleanup() {
   echo ""
   echo "Cleaning up all start_test.sh instances..."

   for pid in "${PIDS[@]}"; do
       if kill -0 "$pid" 2>/dev/null; then
           echo "Killing PID $pid"
           kill -SIGTERM "$pid"
       fi
   done

   wait
   echo "All test processes terminated."
   exit 0
}

trap cleanup SIGINT SIGTERM

# Start start_test.sh for each directory, with output redirected to logfile
for dir in "${DIRS[@]}"; do
   dir_suffix=$(basename "$dir")
   log_file="logs/${HOSTNAME}_start_test_${dir_suffix}.log"
   echo "Starting test for directory: $dir (log: $log_file)"
   ./start_test.sh "$dir" > "$log_file" 2>&1 &
   PIDS+=($!)
   sleep 1
done

echo "All tests started. Press Ctrl+C to stop."

# Keep script running to allow trap to catch signal
while true; do
   sleep 1
done
