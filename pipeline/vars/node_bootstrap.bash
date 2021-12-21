#!/bin/bash

# As we are dynamically creating the Jenkins node using the OpenStack plugin,
# we require additional configurations to enable us to use the system for
# executing the cephci test suites. This script performs the required changes.

echo "Initialize Node"
sudo yum install -y git-core
sudo yum install -y zip unzip
sudo yum install -y p7zip

# Workaround: Disable IPv6 to have quicker downloads
sudo sysctl -w net.ipv6.conf.eth0.disable_ipv6=1

# Mount reesi for storing logs
if [ ! -d "/ceph" ]; then
    echo "Mounting ressi004"
    sudo mkdir -p /ceph
    sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /ceph
fi

if [ ${1:-0} -ne 0 ]; then
  exit 0
fi

sudo yum install -y wget python3
# Copy the config from internal file server to the Jenkins user home directory
wget http://magna002.ceph.redhat.com/cephci-jenkins/.cephci.yaml -O ${HOME}/.cephci.yaml

# Install cephci pre-requisistes
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
.venv/bin/pip install git+https://gitlab.cee.redhat.com/ccit/reportportal/rp_preproc.git@rpv5
deactivate

# Install rclone
curl https://rclone.org/install.sh | sudo bash || echo 0
mkdir -p ~/.config/rclone
wget http://magna002.ceph.redhat.com/cephci-jenkins/.ibm-cos.conf -O ~/.config/rclone/rclone.conf

echo "Done bootstrapping the Jenkins node."
