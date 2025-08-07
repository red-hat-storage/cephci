#!/bin/bash

nodes=("storage-scale-ces-001" "storage-scale-ces-002" "storage-scale-ces-003")
log_file="/var/log/nfs_restart.log"
last_node=""

while true; do
   random_node=${nodes[$RANDOM % ${#nodes[@]}]}

   # Avoid selecting the same node twice in a row
   if [[ "$random_node" == "$last_node" ]]; then
       continue
   fi
   last_node="$random_node"

   {
       echo "=================================================================="
       echo "[$(date '+%Y-%m-%d %H:%M:%S')] [HOST: $(hostname)] Starting new NFS test cycle"
       echo "=================================================================="

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stopping NFS on $random_node..."
       mmces service stop nfs -N "$random_node"

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 60 seconds..."
       sleep 60

       # Get CES IP of the stopped node
       IP=$(mmlscluster --ces | grep storage-scale-ces | grep "$random_node" | awk '{print $4}')

       if [[ -z "$IP" ]]; then
           echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to retrieve CES IP for $random_node"
           echo "Skipping IP move and proceeding to restart NFS..."
       else
           # Get list of remaining nodes (not the one we stopped)
           remaining_nodes=($(mmlscluster --ces | grep "storage-scale-ces" | grep -v "$random_node" | awk '{print $2}'))

           # Select a random node from the remaining ones
           failover_node=${remaining_nodes[$RANDOM % ${#remaining_nodes[@]}]}

           echo "[$(date '+%Y-%m-%d %H:%M:%S')] Moving CES IP $IP to $failover_node..."
           mmces address move --ces-ip "$IP" --ces-node "$failover_node"
       fi

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 180 seconds..."
       sleep 180

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting NFS on $random_node..."
       mmces service start nfs -N "$random_node"

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 180 seconds..."
       sleep 180

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rebalancing CES addresses..."
       mmces address move --rebalance

#   echo "[$(date '+%Y-%m-%d %H:%M:%S')] Moving CES IP $IP to $random_node..."
#   mmces address move --ces-ip "$IP" --ces-node "$random_node"

       echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 300 seconds before next cycle..."
       echo "--------------------------------------------------"
       sleep 300
   } >> "$log_file" 2>&1
done
