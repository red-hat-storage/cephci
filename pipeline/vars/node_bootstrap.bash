#!/bin/bash

# As we are dynamically creating the Jenkins node using the OpenStack plugin,
# we require additional configurations to enable us to use the system for
# executing the cephci test suites. This script performs the required changes.

# Options
#   0 -> Installs python along with cephci required packages
#   1 -> Configures the agent with necessary CI packages
#   2 -> 0 + 1 along with deploying postfix package
#   3 -> 0 + 1 along with rclone package

echo "Initialize Node"
# Workaround: Disable IPv6 to have quicker downloads
sudo sysctl -w net.ipv6.conf.eth0.disable_ipv6=1

sudo yum install -y git-core zip unzip
sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
sudo yum install -y p7zip

# Mount reesi for storing logs
if [ ! -d "/ceph" ]; then
    echo "Mounting ressi004"
    sudo mkdir -p /ceph
    sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /ceph
fi

if [ ${1:-0} -ne 1 ]; then
    sudo yum install -y wget python3
    # Copy the config from internal file server to the Jenkins user home directory
    wget http://magna002.ceph.redhat.com/cephci-jenkins/.cephci.yaml -O ${HOME}/.cephci.yaml

    # Install cephci prerequisites
    rm -rf .venv
    python3 -m venv .venv
    .venv/bin/python -m pip install --upgrade pip
    .venv/bin/python -m pip install -r requirements.txt
fi

# Monitoring jobs have a need to send email using smtplib that requires postfix
if [ ${1:-0} -eq 2 ]; then
    postfix_rpm=$(rpm -qa | grep postfix | wc -l)
    if [ ${postfix_rpm} -eq 0 ]; then
        sudo yum install -y postfix
    fi
    systemctl is-active --quiet postfix || sudo systemctl restart postfix
fi

# Post results workflow requires to sync from COS
if [ ${1:-0} -eq 3 ]; then
    # Install rclone
    curl https://rclone.org/install.sh | sudo bash || echo 0
    mkdir -p ${HOME}/.config/rclone
    wget http://magna002.ceph.redhat.com/cephci-jenkins/.ibm-cos.conf -O ${HOME}/.config/rclone/rclone.conf
fi

echo "Done bootstrapping the Jenkins node."
