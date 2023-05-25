from cli import Cli
from cli.exceptions import NotSupportedError


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


class Container(Cli):
    """This module provides CLI interface for container operations"""

    def __init__(self, nodes, package="podman"):
        super(Container, self).__init__(nodes)
        self.base_cmd = package

    def run(
        self,
        image=None,
        rm=None,
        env=None,
        volume=None,
        ports=None,
        detach_key=None,
        restart=None,
        detach=False,
        cmds=None,
    ):
        """Executes the provided command using podman
        Args
            image (str): Image name
            env (list): List of environment variables
            volume (list): List of volumes
            ports (list): List of ports
            rm (list): List of entries to be removed
            detach (bool): Detach flag
            restart (str): Restart flag
            cmds (str): Other commands to be executed
        """
        if not image and not rm:
            raise NotSupportedError("Image name or rm option needs to be provided")

        cmd = f"{self.base_cmd} run"

        if restart:
            cmd += f" --restart={restart}"

        if rm:
            cmd += f" --rm {rm}"
        else:
            cmd += f" {image}"  # Add image only if not an rm operation

        if env:
            env = env if type(env) in (list, tuple) else [env]
            env = " -e ".join(env)
            cmd += f" {env}"

        if volume:
            volume = volume if type(volume) in (list, tuple) else [volume]
            volume = " -v ".join(volume)
            cmd += f" {volume}"

        if ports:
            ports = ports if type(ports) in (list, tuple) else [ports]
            ports = " -p ".join(ports)
            cmd += f" {ports}"

        if detach_key:
            detach_key = (
                detach_key if type(detach_key) in (list, tuple) else [detach_key]
            )
            detach_key = " -d ".join(detach_key)
            cmd += f" {detach_key}"

        if detach:
            cmd += " -d"

        if rm:
            rm = rm if type(rm) in (list, tuple) else [rm]
            rm = " --rm ".join(rm)
            cmd += f" {rm}"

        if cmds:
            cmd += f" {cmds}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ContainerRegistryError(f"Failed to run {cmd} on container")
