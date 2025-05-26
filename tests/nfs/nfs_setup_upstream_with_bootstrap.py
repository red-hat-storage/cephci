from cli.utilities.configure import SSH_ID_RSA_PUB, SSH_KEYGEN, SSHPASS
from utility.log import Log

log = Log(__name__)
SSH = "~/.ssh"
SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
CEPH_PUB_KEY = "/etc/ceph/ceph.pub"


def exec_cmds(installer, cmds):
    for cmd in cmds:
        print("=" * 20)
        print("Executing cmd  : ",cmd)
        out = installer.exec_command(cmd=cmd, sudo=True)
        print("\n\n OUT: ", out)
        print("=" * 20)
        print("\n")

def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    config = kw.get("config")
    installer = ceph_cluster.get_nodes("installer")[0]

    clients = ceph_cluster.get_nodes("client")
    checkout_patch = False
    servers = set(ceph_cluster.get_nodes("")) - set(clients)

    # Setup Passwordless SSH to other nodes
    installer.exec_command(
        sudo=True, cmd=SSH_KEYGEN, verbose=True
    )

    # Install required packages
    cmds = ['dnf -y install centos-release-ceph epel-release',
            'yum install -y sshpass git bison cmake dbus-devel flex gcc-c++ krb5-devel libacl-devel libblkid-devel '
            'libcap-devel redhat-rpm-config rpm-build xfsprogs-devel',
            'yum install --enablerepo=crb -y libnsl2-devel libnfsidmap-devel libwbclient-devel userspace-rcu-devel',
            'yum install -y libcephfs-devel',
            'rm -rf nfs-ganesha']
    exec_cmds(installer, cmds)

    # Clone the repo

    if checkout_patch:
        cmds = ['git init $(basename "ffilz/nfs-ganesha")',
                'cd $(basename "ffilz/nfs-ganesha"); git fetch --depth=2 https://review.gerrithub.io/ffilz/nfs-ganesha refs/changes/59/1212659/2',
                'cd $(basename "ffilz/nfs-ganesha"); git checkout -b refs/changes/59/1212659/2 FETCH_HEAD']
    else:
        cmds = ['git clone --depth=1 https://review.gerrithub.io/ffilz/nfs-ganesha',
                'cd $(basename "ffilz/nfs-ganesha"); git fetch origin refs/heads/next && git checkout FETCH_HEAD; '
                ]
    cmds.append('cd $(basename "ffilz/nfs-ganesha"); git submodule update --recursive --init || git submodule sync ;')
    exec_cmds(installer, cmds)

    # Build nfs-ganesha
    cmds = ['cd $(basename "ffilz/nfs-ganesha"); [ -d build ] && rm -rf build ; mkdir build ; cd build ; '
            '( cmake ../src -DCMAKE_BUILD_TYPE=Maintainer -DUSE_FSAL_GLUSTER=OFF -DUSE_FSAL_CEPH=ON -DUSE_FSAL_RGW=OFF'
            ' -DUSE_DBUS=ON -DUSE_ADMIN_TOOLS=ON && make) || touch FAILED ; make install']
    exec_cmds(installer, cmds)

    # Install and configure ceph
    cmds = ['dnf install -y cephadm',
            'cephadm add-repo --release squid',
            'dnf install -y ceph',
            f'cephadm bootstrap --mon-ip {installer.ip_address}',
            'sleep 30',
            'ceph fs volume create cephfs',
            'touch /etc/ganesha/ganesha.conf']
    exec_cmds(installer, cmds)

    for node in clients:
        node.exec_command(cmd="yum install -y nfs-utils wget", sudo=True)

    # Setup Passwordless SSH
    for node in ceph_cluster.get_nodes(""):
        if node != installer:

            # cmd = "{} {}".format(SSHPASS.format(node.root_passwd),
            #                      SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.ip_address)
            #                      )
            cmd = f"sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no -f -i ~/.ssh/id_rsa.pub root@{node.ip_address}"
            print(cmd)
            installer.exec_command(cmd=cmd, sudo=True, verbose=True)

    # Copy ceph.pub key
    for node in ceph_cluster.get_nodes(""):
        if node != installer:
            cmd = f"ssh-copy-id -f -i /etc/ceph/ceph.pub root@{node.ip_address}"
            installer.exec_command(cmd=cmd, sudo=True, verbose=True)
            cmd = "yum install -y podman lvm2"
            node.exec_command(cmd=cmd, sudo=True)
    return 0

