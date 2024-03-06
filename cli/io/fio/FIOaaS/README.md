# ceph-fio-docker

The end goal is to support fio for RBD and CephFs workloads.

Currently RBD is supported.

## RBD usage on rbd client machine

[optional] Install docker in RHEL machines ( RBD client )

```bash
# yum config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo
# yum install docker-ce -y
# systemctl enable --now docker
```

Map rbd image as a block device

```bash
rbd map <pool>/<image>
lsblk
```

Cleanup the block device 

```bash
mkfs.ext4 /dev/rbd<x>
```

Run dockerised fio for whole block mapped as directory in docker container

```bash
docker run  --name fio-aas-test  --mount='type=volume,dst=/external,volume-driver=local,volume-opt=device=/dev/rbd1,volume-opt=type=ext4' pavankumarag/ceph-fio-aas:1.0.3 /fio_files/default_directory.fio
```

OR

Run dockerised fio for a particular file mapped as a file in docker container

```bash
mount /dev/rbd<x> /mnt/rbd1
chown ceph:ceph /mnt/rbd1
touch /mnt/rbd1/docker_fs
docker run  --name fio_test --mount type=bind,source=/mnt/rbd1/docker_fs,target=/external_fs pavankumarag/ceph-fio-aas:1.0.3 /fio_files/default_file.fio
```