#!/bin/bash

# This script will use Teuthology framework to reimage
# given set of nodes to the os version required
# This script accepts the hostnames of the nodes as arg

# To invoke this script, pass the argument as below
# => bm_node_reimage.bash node1 node2 node3 os-version:9.0
# last parameter 'os-version:<version>' defines the version to
# which the nodes has to be reimaged to

unlock_and_lock_node()
{
  node=$1
  curr_user=$2
  echo "The node $node is currenlty locked by $curr_user"
  echo "Performing unlock"
  cmd="source $v_env_path/bin/activate;teuthology-lock --unlock --owner $curr_user $node;deactivate"
  res=$(eval $cmd)

  # Now that the node is unlocked, perform lock
  root_user="jenkins@magna006"
  cmd="source $v_env_path/bin/activate;teuthology-lock --lock --owner root_user $node;deactivate"
  res=$(eval $cmd)

  # Verify whether the lock operation succeeded
  cmd="source $v_env_path/bin/activate;teuthology-lock --list $node |
  sed -nr 's/.*locked_by\": \"(.*)\",.*/\1/p';deactivate"
  curr_user=$(eval $cmd)
  if ["$curr_user" != "$root_user" ]; then
    echo "Failed to unlock and lock the node. Exiting!!"
    exit 1
  else
    echo "Successfully relocked machine under $root_user"
  fi
}


v_env_path="/root/teuthology/virtualenv"

# For each node, to perform Teuthology reimage, it has
# to be locked under the current user's name
for arg in "$@"
do
    if [[ "$arg" == *"os-version"* ]]; then
      os_param=$arg
    else
      # Get current user
      host=$arg
      cmd="source $v_env_path/bin/activate;teuthology-lock --list $host | sed -nr 's/.*locked_by\": \"(.*)\",.*/\1/p';deactivate"
      curr_user=$(eval $cmd)
      unlock_and_lock_node $host $curr_user
      nodes+=" "$host
    fi
done

# Perform reimage on the nodes.
# Teuthology supports reimaging of multiple nodes at a time
os_type='rhel'
res=(${os_param//:/ })
os_version=${res[1]}
cmd="source $v_env_path/bin/activate;teuthology-reimage \
--os-type $os_type --os-version $os_version $nodes;deactivate"
res=$(eval $cmd)

# Set root password and Update /etc/ssh/sshd_config with "PermitRootLogin Yes"
for node in "$@"
do
  cmd='ssh -i ~/.ssh/id_rsa ubuntu@$node \'echo "passwd" | \
  sudo passwd --stdin root;grep -qxF "PermitRootLogin yes" /etc/ssh/sshd_config || \
  echo "PermitRootLogin yes" | "sudo tee -a /etc/ssh/sshd_config;sudo service sshd restart'"
  res=$(eval $cmd)
done


#Perform wipefs on nodes where cephfs entries are found
# root@argo026 ~]# lsblk
# NAME                             MAJ:MIN RM   SIZE RO TYPE MOUNTPOINT
# sda                              8:0    0 447.1G  0 disk
# └─sda1                           8:1    0 447.1G  0 part /
# sdb                              8:16   0   1.8T  0 disk
# └─ceph--41d33927--a3ff...        253:1    0   1.8T  0 lvm
# nvme0n1                          259:0    0 931.5G  0 disk
# └─ceph--7050dc1e--428d-...       253:0    0 931.5G  0 lvm
for node in "$@"
do
  # Get all the disks
  cmd="ssh -i ~/.ssh/id_rsa ubuntu@$node lsblk -o NAME -d"
  get_all_disks=$(eval $cmd)

  # Get the root disk
  cmd="ssh -i ~/.ssh/id_rsa ubuntu@$node lsblk -oMOUNTPOINT,PKNAME -rn | awk '$1 ~ /^\/$/ { print $2 }'"
  root_disk=$(eval $cmd)

  for disk in $get_all_disks
  do
    if [[ "$disk" = "NAME" ]] || [[ "$disk" = $root_disk ]]; then
        :
    else
        # Perform wipefs on the non root disk
        cmd="ssh -i ~/.ssh/id_rsa ubuntu@$node wipefs -a --force /dev/$disk"
        res=$(eval $cmd)
    fi
  done

  echo "Completed wipefs for $node"
done

# A restart is required after the above steps
