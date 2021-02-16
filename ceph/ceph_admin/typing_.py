"""Custom typing objects to avoid circular imports."""
from typing import Dict, List

from typing_extensions import Protocol

from ceph.ceph import Ceph, CephInstaller


class CephAdmProtocol(Protocol):
    """CephAdm static duck typing hint to be used with mixin."""

    cluster: Ceph
    config: Dict
    installer: CephInstaller

    def read_cephadm_gen_pub_key(self):
        ...

    def distribute_cephadm_gen_pub_key(self, nodes=None):
        ...

    def set_tool_repo(self):
        ...

    def install(self, **kwargs: Dict) -> None:
        ...


class OrchProtocol(CephAdmProtocol, Protocol):
    """Orch protocol object for supporting static duck typing hints."""

    direct_calls: List[str]

    def shell(
        self: CephAdmProtocol,
        args: List[str],
        check_status: bool = True,
        timeout: int = 300,
    ):
        ...

    def get_role_service(self, service_name: str) -> str:
        ...

    def check_service_exists(
        self, service_name: str, ids: List[str], timeout: int = 300, interval: int = 5
    ) -> bool:
        ...

    def check_service(
        self,
        service_name: str,
        timeout: int = 300,
        interval: int = 5,
        exist: bool = True,
    ) -> bool:
        ...


class ServiceProtocol(OrchProtocol, Protocol):
    """Base service protocol for supporting static duck typing hints."""

    SERVICE_NAME: str
