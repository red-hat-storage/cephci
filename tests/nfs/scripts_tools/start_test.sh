#!/bin/bash

if [ $# -ne 1 ]; then
   echo "Usage: $0 <directory-path>"
   exit 1
fi

DIR=$1
SCRIPT_START=$(date +%s)
PIDS=()
loop_counter=1

# Cleanup on exit
cleanup() {
   echo ""
   echo "Cleaning up..."
   for pid in "${PIDS[@]}"; do
       if kill -0 "$pid" 2>/dev/null; then
           kill -9 "$pid"
       fi
   done

   pkill -f "read_lookc_thr $DIR"
   pkill -f "write_lookc_thr $DIR"

   SCRIPT_END=$(date +%s)
   TOTAL_RUNTIME=$((SCRIPT_END - SCRIPT_START))
   echo "==== Script terminated ===="
   echo "Total script runtime: ${TOTAL_RUNTIME}s"
   exit 0
}

trap cleanup SIGINT SIGTERM

# Start initial read process
./read_lookc_thr "$DIR" > /dev/null 2>&1 &
PIDS+=($!)
echo "Started initial read_lookc_thr in background (PID: $!)"

while true; do
   LOOP_START=$(date +%s)

   echo "----- Loop $loop_counter -----"

   ./read_lookc_thr "$DIR" > /dev/null 2>&1 &
   PIDS+=($!)
   ./write_lookc_thr "$DIR" > /dev/null 2>&1 &
   PIDS+=($!)

   sleep 2

   if (( RANDOM % 2 == 0 )); then
       echo "Killing all read_lookc_thr processes..."
       pkill -f "read_lookc_thr $DIR"
   else
       echo "Killing all write_lookc_thr processes..."
       pkill -f "write_lookc_thr $DIR"
   fi

   LOOP_END=$(date +%s)
   SCRIPT_NOW=$(date +%s)
   LOOP_RUNTIME=$((LOOP_END - LOOP_START))
   SCRIPT_RUNTIME=$((SCRIPT_NOW - SCRIPT_START))

   echo "Loop $loop_counter runtime: ${LOOP_RUNTIME}s | Total script runtime: ${SCRIPT_RUNTIME}s"
   echo ""

   ((loop_counter++))
   sleep 1
done
