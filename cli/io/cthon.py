from ceph.ceph import CommandFailed
from cli import Cli
from cli.utilities.packages import Package, SubscriptionManager
from cli.utilities.utils import git_clone, make
from utility.log import Log

log = Log(__name__)


class Cthon(Cli):
    """This module provides CLI support for cthon tests"""

    def __init__(self, client):
        super(Cthon, self).__init__(client)
        self.client = client
        self.dir = "cthontest_dir/"

    def install_cthon_packages(self):
        """Return the packages required for cthon tests"""
        try:
            Package(self.client).install(
                "git gcc nfs-utils time make libtirpc-devel",
            )
        except Exception as e:
            raise CommandFailed(
                "Failed to install cthon packages \n error: {0}".format(e)
            )

    def remove_cthon_packages(self):
        """Remove the packages required for cthon tests"""
        try:
            Package(self.client).remove(
                "gcc libtirpc-devel",
            )
        except Exception as e:
            raise CommandFailed(
                "Failed to remove cthon packages \n error: {0}".format(e)
            )

    def add_cthon_repos(self):
        """Add cthon repos"""
        try:
            if not self._is_subscription_registered():
                log.info("Skipping adding cthon repos since system is not subscribed")
            else:
                SubscriptionManager(self.client).repos.enable(
                    "codeready-builder-for-rhel-$(rpm -E %rhel)-$(arch)-rpms"
                )
            return True
        except Exception as e:
            raise CommandFailed("Failed to add cthon repos \n error: {0}".format(e))

    def remove_cthon_repos(self):
        """Remove cthon repos"""
        try:
            if not self._is_subscription_registered():
                log.info("Skipping removing cthon repos since system is not subscribed")
            else:
                SubscriptionManager(self.client).repos.disable(
                    "codeready-builder-for-rhel-$(rpm -E %rhel)-$(arch)-rpms"
                )
            return True
        except Exception as e:
            raise CommandFailed("Failed to remove cthon repos \n error: {0}".format(e))

    def install_dependencies(self):
        """Install dependencies for cthon tests"""
        try:
            # Install cthon packages
            git_clone(
                node=self.client,
                git_link="git://git.linux-nfs.org/projects/steved/cthon04.git",
                dir=self.dir,
            )

            self.add_cthon_repos()
            self.install_cthon_packages()

            make(node=self.client, directory=self.dir)
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
        pwd = self.client.exec_command(cmd="pwd", timeout=600)[0].strip()

        cmd = "cd {4}/{3};./server -a -p {0} -m {1} {2}".format(
            export_psudo_path, mount_dir, server_node_ip, self.dir, pwd
        )
        out, err = self.client.exec_command(cmd=cmd, timeout=600, sudo=True)
        return out, err

    def cleanup(self):
        """Cleanup cthon test"""
        try:
            self.client.exec_command(cmd="rm -rf {0}".format(self.dir), timeout=600)
            self.remove_cthon_repos()
            self.remove_cthon_packages()

        except Exception as e:
            raise CommandFailed("Failed to cleanup cthon test \n error: {0}".format(e))

    def _is_subscription_registered(self):
        """Identify if the system is subscribed or not"""
        try:
            cmd = (
                "subscription-manager status | grep 'Overall Status' | awk '{print $3}'"
            )
            out, _ = self.client.exec_command(sudo=True, cmd=cmd)
            status = out.strip()
            log.info("Subscription status output: {}".format(status))
            # In RHEL-9, "Disabled" means registered
            # In RHEL-10, "Registered" means registered
            is_subscribed = status.lower() in ["registered", "disabled"]
            log.info(
                "System is {}".format(
                    "subscribed" if is_subscribed else "not subscribed"
                )
            )
            return is_subscribed
        except Exception as e:
            raise CommandFailed(
                "Failed to identify subscription status \n error: {0}".format(e)
            )
