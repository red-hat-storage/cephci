#!/bin/bash

# Baremetal node preparation helpers.
#
# Authentication is public-key only. sshpass and password arguments have been
# removed: every function takes an optional private key path as its third
# argument and falls back to the agent / default keys when it is omitted.
#
# Arguments common to most functions:
#   node:     FQDN or IP address of the node
#   username: Remote user (default: "root")
#   key:      Path to the SSH private key (optional)

SSH_COMMON_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o BatchMode=yes)

function _ssh {
  # Run a command over ssh using key authentication.
  #   $1 node, $2 username, $3 key (may be empty), $4.. command
  local node="$1"; shift
  local username="$1"; shift
  local key="$1"; shift

  local opts=("${SSH_COMMON_OPTS[@]}")
  if [ -n "${key}" ]; then
    opts+=(-i "${key}")
  fi

  ssh "${opts[@]}" "${username}@${node}" "$@"
}

function cleanup_node {
  # This function is to cleanup any ceph residuals.
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"
  echo "Clean-up ceph residuals on ${node}"
  output=$(_ssh "${node}" "${username}" "${key}" 'sudo rm -rf /etc/ceph /var/lib/ceph /var/log/ceph')
  echo "Status : ${output}"
}

function authorize_key {
  # Authorize the CI public key for root on the node, using an existing
  # key-based login as the provisioning user. Replaces the old flow that set
  # a root password and enabled PermitRootLogin yes.
  #   $1 node, $2 provisioning username, $3 provisioning key (optional)
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"
  local pub_key_file="${SSH_PUBLIC_KEY:-${HOME}/.ssh/id_rsa.pub}"

  if [ ! -f "${pub_key_file}" ]; then
    echo "ERR: public key ${pub_key_file} not found; set SSH_PUBLIC_KEY"
    exit 2
  fi

  local pub_key
  pub_key=$(cat "${pub_key_file}")

  _ssh "${node}" "${username}" "${key}" "sudo install -d -m 700 /root/.ssh; \
    grep -qxF '${pub_key}' /root/.ssh/authorized_keys 2>/dev/null || \
    echo '${pub_key}' | sudo tee -a /root/.ssh/authorized_keys > /dev/null; \
    sudo chmod 600 /root/.ssh/authorized_keys"
}

function initial_setup {
  # Authorize the CI key for root and make sure root login is key-only.
  local node="$1"
  local key="${2:-}"

  authorize_key "${node}" "ubuntu" "${key}"

  _ssh "${node}" "ubuntu" "${key}" 'grep -qxF "PermitRootLogin prohibit-password" /etc/ssh/sshd_config || \
  echo "PermitRootLogin prohibit-password" | sudo tee -a /etc/ssh/sshd_config'
  _ssh "${node}" "ubuntu" "${key}" 'sudo systemctl restart sshd &'
  sleep 2
}

function initial_setup_dsal {
  # Same as initial_setup, for nodes already reachable as root by key.
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"
  KNOWN_HOSTS_FILE="$HOME/.ssh/known_hosts"
  if ssh-keygen -F "${node}" -f "$KNOWN_HOSTS_FILE" >/dev/null; then
    echo "Host already exists in known_hosts."
  else
    ssh-keyscan -H "${node}" >> "$KNOWN_HOSTS_FILE"
  fi

  authorize_key "${node}" "${username}" "${key}"

  _ssh "${node}" "${username}" "${key}" 'grep -qxF "PermitRootLogin prohibit-password" /etc/ssh/sshd_config || \
  echo "PermitRootLogin prohibit-password" | sudo tee -a /etc/ssh/sshd_config'
  _ssh "${node}" "${username}" "${key}" 'sudo systemctl restart sshd &'
  sleep 2
}

function wipe_drives {
  # This function wipes all data disks clean on a given node.
  # The function first retrieves the list of data disks on the node using lsblk.
  # It then identifies the root disk by looking for the disk that has the / mount point.
  # The function then iterates through all disks except the root disk and uses the wipefs command to wipe them clean.
  # If the root disk cannot be found or more than one root disk is found, an error message is printed
  # and the function exits.
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"
  echo "Wipe all data disks clean."

  # Find all disks and its type
  disks=$(_ssh "${node}" "${username}" "${key}" -q "lsblk -ndo name,type | awk '{print \$1\",\"\$2}'")

  # Find root disk device
  root_disk=$(_ssh "${node}" "${username}" "${key}" -q "sudo findmnt -v -n -T /boot -o SOURCE")
  if [ -z "${root_disk}" ]; then
      echo "ERR: Unable to find root disk on ${node}"
      exit 2
  fi

  if [ "$(echo ${root_disk} | wc -l)" != "1" ]; then
      echo "ERR: More than one root disk found on ${node}"
      exit 2
  fi

  mapfile -t arr < <(printf "%s\n" "$disks")

  for disk in "${arr[@]}" ; do
    IFS=',' read -r name type <<< "$disk"
    echo "name: $name, type: $type"

    # shellcheck disable=SC2076
    # Skip wipefs for root, cd-rom.,etc disks
    if [[ "${root_disk}" =~ "/dev/${name}" || "${type}" != "disk" ]]; then
      echo "Skipping this disk :  disk - ${name} , type - ${type}"
      continue
    else
      _ssh "${node}" "${username}" "${key}" "wipefs -a --force /dev/${name}"
    fi
  done
}

# This function sets the hostname of a node to its shortname and cleans the default repo files to avoid conflicts.
function set_hostnames_repos {
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"
  echo 'Setting the systems to use shortnames'
  _ssh "${node}" "${username}" "${key}" "sudo hostnamectl set-hostname ${node}"
  _ssh "${node}" "${username}" "${key}" "sudo echo ${node} > /etc/hostname"
  _ssh "${node}" "${username}" "${key}" 'sudo sed -i "s/$(hostname)/$(hostname -s)/g" /etc/hosts'

  echo 'Cleaning default repo files to avoid conflicts'
  _ssh "${node}" "${username}" "${key}" 'sudo rm -f /etc/yum.repos.d/*; sudo yum clean all'
}

function remove_repos {
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"

  echo 'Cleaning default repo files to avoid conflicts'
  _ssh "${node}" "${username}" "${key}" 'sudo rm -f /etc/yum.repos.d/*; sudo yum clean all'

}

function reboot_node {
  local node="$1"
  local username="${2:-root}"
  local key="${3:-}"

  echo 'Rebooting the node'
  _ssh "${node}" "${username}" "${key}" 'sudo reboot'

}

function wait_for_node {
  local node="$1"
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
  local key="${3:-}"
  echo "Verifying wipefs on ${node}"
  output=$(_ssh "${node}" "${username}" "${key}" 'sudo lsblk -o name,fstype')
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
