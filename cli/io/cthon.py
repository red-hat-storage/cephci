from ceph.ceph import CommandFailed
from cli import Cli


class Cthon(Cli):
    """This module provides CLI support for cthon tests"""

    def __init__(self, client_node):
        super(Cthon, self).__init__(client_node)
        self.client_node = client_node

    def _install_dependencies(self):
        """Install dependencies for cthon tests"""
        cmd = (
            "git clone --depth=1 git://git.linux-nfs.org/projects/steved/cthon04.git;"
            "chmod 755 cthon04/;"
            "cd cthon04/;"
            "sudo subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms;"
            "yum -y install git gcc nfs-utils time make libtirpc-devel;"
            "make"
        )
        out, err = self.client_node.exec_command(cmd=cmd, sudo=True, timeout=600)
        if "FailureException" in out:
            raise CommandFailed(
                "Failed to install dependencies for cthon tests \n error: {0}".format(
                    err
                )
            )
        return True

    def execute_cthon(self, export_psudo_path, mount_dir, server_node_ip):
        """Run cthon test command
        export_psudo_path: export path with psudo ex: NFS export "/export"
        mount_dir: mount path
        server_node_ip: server node ip ex: ip of nfs node
        """
        # Install dependencies
        self._install_dependencies()
        cmd = "cd cthon04/; ./server -a -p {0} -m {1} {2}".format(
            export_psudo_path, mount_dir, server_node_ip
        )
        out, err = self.client_node.exec_command(cmd=cmd, sudo=True, timeout=600)
        return out, err

    def cleanup(self):
        """Cleanup cthon test"""
        out, err = self.client_node.exec_command(
            cmd="rm -rf cthon04", sudo=True, timeout=600
        )
        if "FailureException" in out:
            raise CommandFailed("Failed to cleanup cthon tests")
        return True
