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
        name=None,
        env=None,
        volume=None,
        ports=None,
        restart=None,
        detach_key=None,
        detach=False,
        cmds=None,
    ):
        """Executes the provided command using podman
        Args
            image (str): Image name
            rm (str): Image name to remove from background
            name (str): Container name
            env (list): List of environment variables
            volume (list): List of volumes
            ports (list): List of ports
            restart (str): Restart flag
            detach_key (list): List of detach operation
            detach (str): Image name to detach
            cmds (str): Other commands to be executed
        """
        if not image and not rm:
            raise NotSupportedError("Image or rm needs to be provided")

        cmd = f"{self.base_cmd} run"

        if name:
            cmd += f" --name {name}"

        if restart:
            cmd += f" --restart={restart}"

        if ports:
            ports = ports if type(ports) in (list, tuple) else [ports]
            ports = " -p ".join(ports)
            cmd += f" -p {ports}"

        if volume:
            volume = volume if type(volume) in (list, tuple) else [volume]
            volume = " -v ".join(volume)
            cmd += f" -v {volume}"

        if env:
            env = env if type(env) in (list, tuple) else [env]
            env = " -e ".join(env)
            cmd += f" -e {env}"

        if detach_key:
            detach_key = (
                detach_key if type(detach_key) in (list, tuple) else [detach_key]
            )
            detach_key = " -d ".join(detach_key)
            cmd += f" {detach_key}"

        if detach:
            cmd += " -d"

        if rm:
            cmd += f" --rm {rm}"

        if image:
            cmd += f" {image}"

        if cmds:
            cmd += f" {cmds}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ContainerRegistryError(f"Failed to run {cmd} on container")

    def pull(self, image):
        """Pull container image

        Args:
            image (str): Container image url
        """

        cmd = f"{self.base_cmd} pull {image}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ContainerRegistryError(f"Failed to pull container image '{image}'")

    def rmi(self, image):
        """Remove container image

        Args:
            image (str): Container image url
        """
        cmd = f"{self.base_cmd} rmi {image}"

        if self.execute(sudo=True, long_running=True, cmd=cmd):
            raise ContainerRegistryError(f"Failed to remove container image '{image}'")

    def inspect(self, image, format=None):
        """Inspect container image

        Args:
            image (str): Container image url
            format (str): String to format
        """
        cmd = f"{self.base_cmd} inspect {image}"
        if format:
            cmd += f" --format={format}"

        out = self.execute(sudo=True, cmd=cmd)
        if not out:
            raise ContainerRegistryError(f"Failed to inspect container image '{image}'")

        return out

    def compare(self, image, version):
        """Compare container image version

        Args:
            image (str): Container image url
            version (str): Container image expected version
        """
        _version = self.inspect(image, format="'{{.Config.Labels.version}}'")[0]
        if _version < version:
            return -1
        elif _version == version:
            return 0
        else:
            return 1

    def ps(self, all=None, filter=None):
        """
        Returns the details of running containers
        Args:
            all (bool): Show all containers
            filter (dict): Key value to filter
        """
        cmd = f"{self.base_cmd} ps"
        if all:
            cmd += " --all"

        if filter:
            for key, val in filter.items():
                cmd += f" --filter {key}={val}"
        return self.execute(sudo=True, cmd=cmd)
