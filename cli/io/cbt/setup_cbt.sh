#!/bin/bash
git clone https://github.com/ceph/cbt.git
sudo yum install blktrace perf valgrind fio pdsh pdsh-rcmd-ssh ceph-common -y
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r cbt/requirements.txt
