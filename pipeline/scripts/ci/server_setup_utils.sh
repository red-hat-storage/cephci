#!/bin/bash

# Function to perform initial setup on node
# Arguments:
#   node: The node to perform setup on
#   password: The password to set for the root user (default: "passwd")
function initial_setup {
  # This function performs initial setup on a given node. It takes two arguments:
  #   node: The name or IP address of the node to perform setup on
  #   password: The password to set for the root user (default: "passwd")
  # The function uses sshpass to login to the node with root privileges and sets the root password to the provided value.
  # It then checks if PermitRootLogin is set to yes in /etc/ssh/sshd_config and if not, adds the setting to the file.
  # Finally, the sshd service is restarted on the node.
  local node="$1"
  local password="${2:-passwd}"
  ssh ubuntu@${node} 'echo "passwd" | sudo passwd --stdin root; \
  grep -qxF "PermitRootLogin yes" /etc/ssh/sshd_config || \
  echo "PermitRootLogin yes" | sudo tee -a /etc/ssh/sshd_config'
  ssh ubuntu@${node} 'sudo systemctl restart sshd &'
  sleep 2
}

function initial_setup_dsal {
  # This function performs initial setup on a given node. It takes two arguments:
  #   node: The name or IP address of the node to perform setup on
  #   password: The password to set for the root user (default: "passwd")
  # The function uses sshpass to login to the node with root privileges and sets the root password to the provided value.
  # It then checks if PermitRootLogin is set to yes in /etc/ssh/sshd_config and if not, adds the setting to the file.
  # Finally, the sshd service is restarted on the node.
  local node="$1"
  local password="${2:-passwd}"
  KNOWN_HOSTS_FILE="$HOME/.ssh/known_hosts"
  if ssh-keygen -F "${node}" -f "$KNOWN_HOSTS_FILE" >/dev/null; then
    echo "Host already exists in known_hosts."
  else
    ssh-keyscan -H "${node}" >> "$KNOWN_HOSTS_FILE"
  fi

  ssh root@${node} 'echo "passwd" | sudo passwd --stdin root; \
  grep -qxF "PermitRootLogin yes" /etc/ssh/sshd_config || \
  echo "PermitRootLogin yes" | sudo tee -a /etc/ssh/sshd_config'
  ssh root@${node} 'sudo systemctl restart sshd &'
  sleep 2
}

# Function to wipe data disks clean
# Arguments:
#   node: The node to wipe disks on
#   password: The password to use to access the node (default: "passwd")
function wipe_drives {
    # This function wipes all data disks clean on a given node. It takes two arguments:
    #   node: The name or IP address of the node to wipe disks on
    #   password: The password to use to access the node (default: "passwd")
    # The function first retrieves the list of data disks on the node using lsblk.
    # It then identifies the root disk by looking for the disk that has the / mount point.
    # The function then iterates through all disks except the root disk and uses the wipefs command to wipe them clean.
    # If the root disk cannot be found or more than one root disk is found, an error message is printed
    # and the function exits.
    local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"
    echo "Wipe all data disks clean."
    disks=$(sshpass -p ${password} ssh ${username}@${node} 'lsblk -o NAME -d | tail -n +2')
    root_disk=$(sshpass -p ${password} ssh ${username}@${node} 'eval $(lsblk -o PKNAME,MOUNTPOINT -P | grep "MOUNTPOINT=\"/\""); echo $PKNAME')

    if [ -z "${root_disk}" ]; then
        echo "ERR: Unable to find root disk on ${node}"
        exit 2
    fi

    if [ "$(echo ${root_disk} | wc -l)" != "1" ]; then
        echo "ERR: More than one root disk found on ${node}"
        exit 2
    fi

    for disk in ${disks} ; do
      # shellcheck disable=SC2076
      if [[ "${root_disk}" =~ "${disk}" ]]; then
        continue
      else
    sshpass -p ${password} ssh ${username}@${node} "wipefs -a --force /dev/${disk}"
    fi
    done
}

# This function sets the hostname of a node to its shortname and cleans the default repo files to avoid conflicts.
# Arguments:
#   node: The node to perform setup on
#   password: The password to set for the root user (default: "passwd")
function set_hostnames_repos {
    local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"
    echo 'Setting the systems to use shortnames'
    sshpass -p ${password} ssh ${username}@${node} 'sudo hostnamectl set-hostname $(hostname -s)'
    sshpass -p ${password} ssh ${username}@${node} 'sudo sed -i "s/$(hostname)/$(hostname -s)/g" /etc/hosts'

    echo 'Cleaning default repo files to avoid conflicts'
    sshpass -p ${password} ssh ${username}@${node} 'sudo rm -f /etc/yum.repos.d/*; sudo yum clean all'
}

function remove_repos {
  local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"

    echo 'Cleaning default repo files to avoid conflicts'
    sshpass -p ${password} ssh ${username}@${node} 'sudo rm -f /etc/yum.repos.d/*; sudo yum clean all'

  }

function reboot_node {
  local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"

    echo 'Cleaning default repo files to avoid conflicts'
    sshpass -p ${password} ssh ${username}@${node} 'sudo reboot'

  }

function wait_for_node {
  local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"
    local max_attempts=10
    local wait_time=60

    echo "Waiting for node ${node} to come back online..."

    for ((attempt=1; attempt<=max_attempts; attempt++)); do
        if ping -c 1 ${node} &> /dev/null; then
            echo "Node ${node} is online"
            return 0
        else
            echo "Node ${node} is still offline. Attempt ${attempt}/${max_attempts}"
            sleep ${wait_time}
        fi
    done

    echo "Node ${node} did not come back online after ${max_attempts} attempts."
    return 1
 }

function verify_disk() {
    local node="$1"
    local username="${2:-root}"
    local password="${3:-passwd}"
    echo "Verifying wipefs on ${node}"
    output=$(sshpass -p ${password} ssh ${username}@${node} 'sudo lsblk -o name,fstype')
    echo "ouput of lsblk ${output}"
    # Check if any block device has a non-empty fstype
    while read -r line; do
        if [[ ! -z $(echo "${line}" | awk '{print $2}') ]]; then
            echo true
            return
        fi
    done <<< "$output"
    echo false
}
