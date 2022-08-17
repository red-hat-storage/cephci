"""Custom typing objects to avoid circular imports."""
from datetime import datetime
from typing import Dict, List

try:
    from typing_extensions import Protocol
except ImportError:
    from typing import Protocol

from ceph.ceph import Ceph, CephInstaller


class CephAdmProtocol(Protocol):
    """CephAdm static duck typing hint to be used with mixin."""

    cluster: Ceph
    config: Dict
    installer: CephInstaller

    def read_cephadm_gen_pub_key(self, ssh_key_path=None):
        ...

    def distribute_cephadm_gen_pub_key(self, ssh_key_path=None, nodes=None):
        ...

    def set_tool_repo(self, repo=None):
        ...

    def install(self, **kwargs: Dict) -> None:
        ...

    def shell(
        self,
        args: List[str],
        base_cmd_args: Dict = None,
        check_status: bool = True,
        timeout: int = 600,
        long_running: bool = False,
    ):
        ...


class OrchProtocol(CephAdmProtocol, Protocol):
    """Orch protocol object for supporting static duck typing hints."""

    direct_calls: List[str]

    def shell(
        self: CephAdmProtocol,
        args: List[str],
        base_cmd_args: Dict = None,
        check_status: bool = True,
        timeout: int = 600,
        long_running: bool = False,
    ):
        ...

    def get_role_service(self, service_name: str) -> str:
        ...

    def get_hosts_by_label(self, label: str) -> List:
        ...

    def check_service(
        self,
        service_name: str,
        timeout: int = 300,
        interval: int = 5,
        exist: bool = True,
    ) -> bool:
        ...

    def op(self, op: str, config: Dict):
        ...

    def verify_status(self, op: str) -> None:
        ...

    def check_service_restart(
        self,
        service_name: str,
        restart_init_time: datetime,
    ) -> bool:
        ...


class DaemonProtocol(OrchProtocol, Protocol):
    """Daemon protocol object for supporting static duck typing hints."""


class ServiceProtocol(OrchProtocol, Protocol):
    """Base service protocol for supporting static duck typing hints."""

    SERVICE_NAME: str
