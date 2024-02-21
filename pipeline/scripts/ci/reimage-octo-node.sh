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
if ! ${REIMAGE_CMD} --os-type rhel --os-version ${OS_VER} ${NODES}; then
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

# Additional setup steps
for node in ${NODES} ; do
    initial_setup "${node}"
    wipe_drives "${node}"
    set_hostnames_repos "${node}"
done
