#!/usr/bin/env bash
#
# This utility performs a reimage operation on the provided set of nodes.
#
# The script allows performs the additional configuration changes on the provided nodes.
#
#   - Enables SSH access to root user
#   - Wipes the data disks
#   - Forces the nodes to use shortname instead of FQDN
#
#   Usage
#       $ bash reimage-octo-node.sh \
#           --platform [8.5|8.6|9.0] \
#           --nodes node1,node2,node3
#
#   Requires
#       .tthlgy python virtual environment in the user's home directory
#
set -eux -o pipefail

source pipeline/scripts/ci/server_setup_utils.sh

# Define the variables used in the script here.
OS_VER=""
NODES=""
REIMAGE_CMD=${HOME}/.tthlg/bin/teuthology-reimage

function usage {
    echo "Usage: ${0} [-p | --platform os_version] [-n | --nodes node1]"
    exit 2
}

CLI_OPTS=$(getopt -o hn:p: --long platform:,nodes:,help -- "$@" )
if [ $? != 0 ] || [ $# != 4 ] ; then
    usage
fi

eval set -- "${CLI_OPTS}"
while true; do
    case ${1} in
        -h | --help) usage ;;
        -n | --nodes) NODES="${2//,/ }"; shift 2 ;;
        -p | --platform) OS_VER="${2}"; shift 2 ;;
        --) shift; break ;;
        *) usage ;;
    esac
done

echo "Initiating reimage of nodes"
# Run reimage command with error handling
if ! sudo ${REIMAGE_CMD} --os-type rhel --os-version ${OS_VER} ${NODES} --owner jenkins-build@magna006; then
    echo "Reimage command failed, continuing with checks..."
fi

# Check OS on each node
nodes_with_mismatch=()
for node in ${NODES} ; do
    echo "Checking OS on ${node}"
    actual_os=$(ssh ubuntu@${node}.ceph.redhat.com 'cat /etc/os-release | grep VERSION_ID' | awk -F '"' '{print $2}')
    if [ "${actual_os}" != "${OS_VER}" ]; then
        echo "Error: OS version mismatch on ${node}. Expected: ${OS_VER}, Actual: ${actual_os}"
        nodes_with_mismatch+=("${node}")
    else
        echo "OS version matches on ${node}: ${actual_os}"
    fi
    echo "-----------------------------------"
done

# Check if any nodes have OS mismatches
if [ ${#nodes_with_mismatch[@]} -gt 0 ]; then
    echo "Nodes with OS version mismatches:"
    printf '%s\n' "${nodes_with_mismatch[@]}"
    exit 1
fi
failed_nodes=()
# Additional setup steps
for node in ${NODES}; do
    initial_setup "${node}"
    set_hostnames_repos "${node}"
    wipe_drives "${node}"
    cleanup_node "${node}"
    reboot_node "${node}"
done

# Check status of all nodes and perform disk verification for online nodes
for node in ${NODES}; do
    if wait_for_node "${node}"; then
        output=$(verify_disk "${node}")
        if [[ $output == true ]]; then
            failed_nodes+=("$node")
        fi
    else
        echo "Failed to bring node ${node} back online after reboot."
        failed_nodes+=("$node")
    fi
done

retry_count=0
while [ ${#failed_nodes[@]} -gt 0 ] && [ ${retry_count} -lt 3 ]; do
    echo "Retrying for failed nodes (Attempt $((retry_count + 1)))"
    new_failed_nodes=()
    for node in "${failed_nodes[@]}"; do
        wipe_drives "${node}"
        cleanup_node "${node}"
        reboot_node "${node}"
    done

    # Check status of all nodes and perform disk verification for online nodes
    for node in "${failed_nodes[@]}"; do
        if wait_for_node "${node}"; then
            output=$(verify_disk "${node}")
            if [[ $output == true ]]; then
                new_failed_nodes+=("$node")
            fi
        else
            echo "Failed to bring node ${node} back online after reboot."
            new_failed_nodes+=("$node")
        fi
    done

    failed_nodes=("${new_failed_nodes[@]}")
    ((retry_count++))
done

# If there are still failed nodes, print them and fail the script
if [[ ${#failed_nodes[@]} -gt 0 ]]; then
    echo "Failed nodes after retry: ${failed_nodes[@]}"
    exit 1
fi
