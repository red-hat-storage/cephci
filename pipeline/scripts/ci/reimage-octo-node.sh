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
${REIMAGE_CMD} --os-type rhel --os-version ${OS_VER} ${NODES}

for node in ${NODES} ; do
    ssh ${node} 'echo "passwd" | sudo passwd --stdin root; \
    grep -qxF "PermitRootLogin yes" /etc/ssh/sshd_config || \
    echo "PermitRootLogin yes" | sudo tee -a /etc/ssh/sshd_config'
    ssh ${node} 'sudo systemctl restart sshd &'
    sleep 2

    echo "Wipe all data disks clean."
    disks=$(ssh ${node} 'lsblk -o NAME -d | tail -n +2')
    root_disk=$(ssh ${node} 'eval $(lsblk -o PKNAME,MOUNTPOINT -P | grep "MOUNTPOINT=\"/\""); echo $PKNAME')

    if [ -z "${root_disk}" ]; then
        echo "ERR: Unable to find root disk on ${node}"
        exit 2
    fi

    if [ "$(echo ${root_disk} | wc -l)" != "1" ]; then
        echo "ERR: More than one root disk found on ${node}"
        exit 2
    fi

    for disk in ${disks} ; do
        if [ "${disk}" != "${root_disk}" ] ; then
            ssh ${node} "wipefs -a --force /dev/${disk}"
        fi
    done

    echo 'Setting the systems to use shortnames'
    ssh ${node} 'sudo hostnamectl set-hostname $(hostname -s).ceph.redhat.com'
    ssh ${node} 'sudo sed -i "s/$(hostname)/$(hostname -s)/g" /etc/hosts'

    echo 'Cleaning default repo files to avoid conflicts'
    ssh ${node} 'sudo rm -f /etc/yum.repos.d/*; sudo yum clean all'
done
