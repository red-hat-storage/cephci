from ceph.ceph import CommandFailed
from cli import Cli
from cli.utilities.packages import Package, Repos
from cli.utilities.utils import change_permission_any_dir, clone_git_repo, make_repo


class Cthon(Cli):
    """This module provides CLI support for cthon tests"""

    def __init__(self, client):
        super(Cthon, self).__init__(client)
        self.client = client

    @property
    def git_dir(self):
        """Return the git directory for cthon"""
        pwd = self.client.exec_command(cmd="pwd")[0].strip("\n")
        return "{0}/cthon04".format(pwd)

    def install_dependencies(self):
        """Install dependencies for cthon tests"""
        try:
            # Install cthon packages
            clone_git_repo(
                node=self.client,
                git_link="git://git.linux-nfs.org/projects/steved/cthon04.git {0}".format(
                    self.git_dir
                ),
                dir=self.git_dir,
            )

            change_permission_any_dir(
                client=self.client, dir_path=self.git_dir, permissions=755
            )

            Repos(self.client, base_cmd="sudo subscription-manager").enable(
                "codeready-builder-for-rhel-9-$(arch)-rpms",
            )

            Package(self.client).install(
                "git gcc nfs-utils time make libtirpc-devel",
            )
            make_repo(node=self.client, directory=self.git_dir)
            return True
        except Exception as e:
            self.cleanup()
            raise CommandFailed(
                "Failed to install dependencies for cthon tests \n error: {0}".format(e)
            )

    def execute_cthon(self, export_psudo_path, mount_dir, server_node_ip):
        """Run cthon test command
        export_psudo_path: export path with psudo ex: NFS export "/export"
        mount_dir: mount path
        server_node_ip: server node ip ex: ip of nfs node
        """
        # Install dependencies

        cmd = "cd {3}; ./server -a -p {0} -m {1} {2}".format(
            export_psudo_path, mount_dir, server_node_ip, self.git_dir
        )
        out, err = self.client.exec_command(cmd=cmd, timeout=600, sudo=True)
        return out, err

    def cleanup(self):
        """Cleanup cthon test"""
        out, err = self.client.exec_command(
            cmd="rm -rf {0}".format(self.git_dir), timeout=600, sudo=True
        )
        if "FailureException" in out:
            raise CommandFailed("Failed to cleanup cthon tests")
        return True
