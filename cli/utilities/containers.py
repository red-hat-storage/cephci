from cli import Cli


class ContainerRegistryError(Exception):
    pass


class Registry(Cli):
    """This module provides CLI interface for container registry operations"""

    def __init__(self, nodes, package="podman"):
        super(Registry, self).__init__(nodes)
        self.base_cmd = package

    def login(
        self, registry, username=None, password=None, authfile=None, tls_verify=False
    ):
        """Login to registry

        Args:
            registry (str): Registry url
            username (str): Registry username
            password (str): Registry password
            authfile (str): File with authentication details
            tls_verify (boot): TLS verification flag
        """
        cmd = f"{self.base_cmd} login"
        if tls_verify:
            cmd += f" --tls-verify={tls_verify}"

        if authfile:
            cmd += f" --authfile {authfile}"
        elif username and password:
            cmd += f" --username {username} --password {password}"
        else:
            raise ContainerRegistryError("Authentication details needs to be provided")

        cmd += f" {registry}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ContainerRegistryError(
                f"Failed to login to container registry '{registry}'"
            )
