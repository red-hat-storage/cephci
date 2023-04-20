from cli import Cli
from cli.utilities.utils import verify_execution_status

RPM_QUERY_ERROR_MSG = "package {} is not installed"


class PackageError(Exception):
    pass


class SubscriptionManagerError(Exception):
    pass


class ReposError(Exception):
    pass


class RpmError(Exception):
    pass


class Package(Cli):
    """This module provides CLI interface for yum/dnf operations"""

    def __init__(self, nodes, manager="yum"):
        super(Package, self).__init__(nodes)
        self.manager = manager

    def info(self, pkg=None):
        """show a package information

        Args:
            pkg (str): package name
        """
        cmd = f"{self.manager} info"
        if pkg:
            cmd += f" {pkg}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def list(self, pkg=None):
        """list a package installed

        Args:
            pkg (str): package name
        """
        cmd = f"{self.manager} list "
        if pkg:
            cmd += f" {pkg}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def install(self, pkg, nogpgcheck=False, env_vars={}):
        """install a package or packages

        Args:
            pkg (str): package need to be installed
            env_vars (dict): dictiory with environment variables
        """
        cmd = ""
        if env_vars:
            for k, v in env_vars.items():
                cmd += f"{k}={v} "

        cmd += f"{self.manager} install -y {pkg}"
        if nogpgcheck:
            cmd += " --nogpgcheck"

        # When multiple nodes are passed, the execute returns a dict with return value
        # for each node Each of these return values has to be checked to ensure that
        # the package is installed in the specified nodes
        out = self.execute(sudo=True, long_running=True, cmd=cmd)
        if isinstance(out, dict):
            if not verify_execution_status(out, pkg):
                raise PackageError("Failed to install package '{pkg}'")
        elif out:
            raise PackageError(f"Failed to install package '{pkg}'")

    def upgrade(self, pkg):
        """upgrade a package or packages

        Args:
            pkg (str): package need to be installed
        """
        cmd = f"{self.manager} upgrade -y {pkg}"

        # When multiple nodes are passed, the execute returns a dict with return value
        # for each node Each of these return values has to be checked to ensure that
        # the package is upgraded in the specified nodes
        out = self.execute(sudo=True, long_running=True, cmd=cmd)
        if isinstance(out, dict):
            if not verify_execution_status(out, pkg):
                raise PackageError("Failed to upgrade package '{pkg}'")
        elif out:
            raise PackageError(f"Failed to upgrade package '{pkg}'")

    def add_repo(self, repo):
        """enable repos with config-manager command

        Args:
            repo (str): repo to be enabled
        """
        cmd = f"yum-config-manager --add-repo {repo}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()

    def clean(self):
        """clean repos"""
        cmd = f"{self.manager} clean all"
        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise PackageError("Failed to clean repositories")


class SubscriptionManager(Cli):
    """This module provides CLI interface for RH Subscription Manager."""

    def __init__(self, nodes):
        super(SubscriptionManager, self).__init__(nodes)

        self.base_cmd = "subscription-manager"
        self.repos = Repos(nodes, self.base_cmd)

    def register(self, username, password, serverurl=None, baseurl=None, force=False):
        """Register system to the Customer Portal or another subscription management service.

        Args:
            username (str): username to use when authorizing against the server
            password (str): password to use when authorizing against the server
            serverurl (str): server URL
            baseurl (str): base URL for content
            force (bool): register the system even if it is already registered
        """
        cmd = f"{self.base_cmd} register --username {username} --password {password}"
        if serverurl:
            cmd += f" --serverurl {serverurl}"

        if baseurl:
            cmd += f" --baseurl {baseurl}"

        if force:
            cmd += " --force"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise SubscriptionManagerError(
                "Failed to register node(s) to subscription manager"
            )

    def unregister(self):
        """Unregister this system from the Customer Portal or another subscription management service."""
        cmd = f"{self.base_cmd} unregister"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise SubscriptionManagerError(
                "Failed to unregister node(s) to subscription manager"
            )

    def status(self):
        cmd = f"{self.base_cmd} status"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out


class Repos(Cli):
    """This module provides CLI interface to perform subscription manager repo operations."""

    def __init__(self, nodes, base_cmd):
        super(Repos, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} repos"

    def enable(self, repos):
        """Enable subscription manager repos.

        Args:
            repos (str|list): repositories to be enabled
        """
        if isinstance(repos, str):
            repos = [str(repos)]

        repos = list(map(lambda repo: f"--enable={repo}", repos))
        cmd = f"{self.base_cmd} {' '.join(repos)}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ReposError(f"Failed to enable repos '{repos}'")

    def disable(self, repos):
        """Enable subscription manager repos.

        Args:
            repos (str|list): repositories to be disabled
        """
        if isinstance(repos, str):
            repos = [str(repos)]

        repos = list(map(lambda repo: f"--disable={repo}", repos))
        cmd = f"{self.base_cmd} {' '.join(repos)}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ReposError(f"Failed to disable repos '{repos}'")

    def list(self, status=None):
        """List all/enabled/disabled repositories

        Args:
            status (str): repositories status (all/enabled/disabled)
        """
        cmd = f"{self.base_cmd} --list"
        if status:
            cmd += f"-{status}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out


class Rpm(Cli):
    """This module provides CLI interface for rpm operations"""

    def __init__(self, nodes):
        super(Rpm, self).__init__(nodes)
        self.base_cmd = "rpm"

    def query(self, pkg):
        """query a package information

        Args:
            pkg (str): package name
        """
        cmd = f"{self.base_cmd} --query {pkg}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            if out[0].strip() == RPM_QUERY_ERROR_MSG.format(pkg):
                return None
            else:
                return out[0].strip()
        else:
            result = {}
            for node in out.keys():
                if out[node][0].strip() == RPM_QUERY_ERROR_MSG.format(pkg):
                    result[node] = None
                else:
                    result[node] = out[node]

            return result
