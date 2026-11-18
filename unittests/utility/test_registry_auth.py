"""Unit tests for utility.registry_auth multi-registry helpers."""

import base64
import json

import pytest

from utility.registry_auth import (
    PODMAN_AUTH_PATH,
    merge_auths,
    merge_registry_into_podman_auth,
    read_podman_auth_file,
    registry_login_args_from_podman_auths,
    write_podman_auth_file,
)

DIGEST = "79c0a55aff338be4196827c5d050384c0302e80ce44c84eb926b6cd7128bbc94"


def _b64(user, passwd):
    return base64.b64encode(f"{user}:{passwd}".encode("ascii")).decode("utf-8")


class FakeRemoteFile:
    def __init__(self, node):
        self.node = node
        self.buffer = ""

    def write(self, content):
        self.buffer += content

    def flush(self):
        pass

    def close(self):
        self.node.files[PODMAN_AUTH_PATH] = self.buffer


class FakeNode:
    hostname = "node1"

    def __init__(self, content=None):
        self.files = {}
        self.commands = []
        if content is not None:
            self.files[PODMAN_AUTH_PATH] = content

    def exec_command(self, cmd, sudo=False, check_ec=False, timeout=None, **kwargs):
        self.commands.append(cmd)
        if cmd.startswith("cat "):
            path = cmd.split(" ", 1)[1]
            if path in self.files:
                return self.files[path], ""
            return "", "cat: No such file or directory"
        if "/dev/null" in cmd:
            self.files.pop(PODMAN_AUTH_PATH, None)
        return "", ""

    def remote_file(self, sudo=False, file_name=None, file_mode=None):
        return FakeRemoteFile(self)


def test_merge_auths_union_and_later_wins():
    assert merge_auths({"a": {"auth": "1"}}, {"b": {"auth": "2"}}) == {
        "a": {"auth": "1"},
        "b": {"auth": "2"},
    }
    assert merge_auths({"a": {"auth": "old"}}, {"a": {"auth": "new"}}) == {
        "a": {"auth": "new"}
    }
    assert merge_auths(None, {}, {"a": {"auth": "1"}}) == {"a": {"auth": "1"}}


def test_registry_login_args_from_auth_field():
    auths = {"cp.icr.io": {"auth": _b64("user", "pass:word")}}
    assert registry_login_args_from_podman_auths(auths, "cp.icr.io") == {
        "registry-url": "cp.icr.io",
        "registry-username": "user",
        "registry-password": "pass:word",
    }


def test_registry_login_args_from_username_password_fields():
    auths = {"registry.stage.redhat.io": {"username": "u", "password": "p"}}
    assert registry_login_args_from_podman_auths(auths, "registry.stage.redhat.io") == {
        "registry-url": "registry.stage.redhat.io",
        "registry-username": "u",
        "registry-password": "p",
    }


@pytest.mark.parametrize(
    "entry",
    [
        {},
        {"auth": ""},
        {"auth": "not-base64!!"},
        {"auth": _b64("user", "")},
        {"identitytoken": "token-only"},
    ],
)
def test_registry_login_args_returns_empty_on_unusable_entries(entry):
    assert registry_login_args_from_podman_auths({"h": entry}, "h") == {}


def test_registry_login_args_unknown_host():
    assert (
        registry_login_args_from_podman_auths({"a": {"auth": _b64("u", "p")}}, "b")
        == {}
    )


def test_read_podman_auth_file_parses_auths():
    node = FakeNode(json.dumps({"auths": {"registry.redhat.io": {"auth": "x"}}}))
    assert read_podman_auth_file(node) == {"registry.redhat.io": {"auth": "x"}}


def test_read_podman_auth_file_missing_or_invalid():
    assert read_podman_auth_file(FakeNode()) == {}
    assert read_podman_auth_file(FakeNode("not json")) == {}
    assert read_podman_auth_file(FakeNode(json.dumps({"auths": []}))) == {}


def test_write_podman_auth_file_round_trip_and_permissions():
    node = FakeNode()
    auths = {"registry.redhat.io": {"auth": "x"}}
    assert write_podman_auth_file(node, auths) is True
    assert read_podman_auth_file(node) == auths
    assert "chmod 600 /etc/ceph/podman-auth.json" in node.commands


def test_write_podman_auth_file_empty_is_noop():
    node = FakeNode()
    assert write_podman_auth_file(node, {}) is False
    assert node.files == {}


def test_merge_registry_into_podman_auth_preserves_existing():
    node = FakeNode(json.dumps({"auths": {"registry.redhat.io": {"auth": "cdn"}}}))
    merge_registry_into_podman_auth(node, "cp.stg.icr.io", "u", "p")
    auths = read_podman_auth_file(node)
    assert auths["registry.redhat.io"] == {"auth": "cdn"}
    assert auths["cp.stg.icr.io"] == {"auth": _b64("u", "p")}


@pytest.mark.parametrize(
    "host,user,passwd", [("", "u", "p"), ("h", "", "p"), ("h", "u", "")]
)
def test_merge_registry_into_podman_auth_ignores_incomplete(host, user, passwd):
    node = FakeNode(json.dumps({"auths": {"a": {"auth": "x"}}}))
    merge_registry_into_podman_auth(node, host, user, passwd)
    assert read_podman_auth_file(node) == {"a": {"auth": "x"}}
