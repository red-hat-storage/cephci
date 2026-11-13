"""V2 ``ceph nvmeof`` command shape with and without mTLS.

``ceph nvmeof`` loads the client certificate from the cephadm cert store
when the gateway has ``enable_auth``. It does not accept the container
CLI certificate flags. Injecting ``--server_address`` on an mTLS command
can fail the handshake (IBMCEPH-16272), so the helper leaves that address
unset unless the caller supplied one.
"""

from ceph.nvmeof.cli.v2.base_cli import BaseCLI


class _Node:
    ip_address = "10.0.66.110"


class _CLI(BaseCLI):
    def __init__(self, mtls, version="20.2.2"):
        super().__init__(_Node(), shell=None)
        self.mtls = mtls
        self.gateway_group = "group1"
        self.ceph_version = version


def _text(cli, **kwargs):
    return " ".join(cli.build_command("subsystem", "add", **kwargs))


def test_mtls_omits_injected_address_and_cert_flags():
    text = _text(_CLI(True), args={"subsystem": "nqn.example"})
    assert "ceph nvmeof subsystem add" in text
    assert "--gw_group group1" in text
    assert "--subsystem nqn.example" in text
    assert "--server_address" not in text
    assert "--traddr" not in text
    assert "--client-key" not in text
    assert "--client-cert" not in text
    assert "--server-cert" not in text


def test_mtls_keeps_an_address_the_caller_set():
    text = _text(
        _CLI(True),
        args={"subsystem": "nqn.example", "server_address": "10.1.2.3"},
    )
    assert "--server_address 10.1.2.3" in text
    assert "10.0.66.110" not in text
    assert "--client-cert" not in text


def test_plaintext_still_targets_this_gateway():
    text = _text(_CLI(False), args={"subsystem": "nqn.example"})
    assert "--server_address 10.0.66.110" in text
    assert "--client-key" not in text
    assert "--client-cert" not in text
    assert "--server-cert" not in text


def test_plaintext_older_release_uses_traddr():
    text = _text(_CLI(False, version="19.2.0"), args={"subsystem": "nqn.example"})
    assert "--traddr 10.0.66.110" in text
    assert "--server_address" not in text
