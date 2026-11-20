"""Guard the key-only SSH contract.

These tests mock paramiko so no network access is needed. They assert that
nothing in the Ceph node path can authenticate with a password, and that the
helpers which used to depend on sshpass now authorize public keys instead.
"""

import inspect
from unittest.mock import MagicMock, patch

import pytest

from ceph.ceph import CephNode, SSHConnectionManager


def _node_kwargs(**overrides):
    kwargs = dict(
        username="cephuser",
        root_username="root",
        private_key_path="/tmp/does-not-exist-key",
        private_key_password=None,
        role=["mon"],
        no_of_volumes=None,
        ip_address="10.0.0.5",
        subnet="10.0.0.0/24",
        private_ip="10.0.0.5",
        hostname="node1.example.com",
        ceph_vmnode=None,
        ceph_nodename="node1",
        id="node1",
    )
    kwargs.update(overrides)
    return kwargs


class TestSSHConnectionManager:
    def test_rejects_password_argument(self):
        """A password must not be accepted, even positionally."""
        with pytest.raises(TypeError):
            SSHConnectionManager("10.0.0.5", "root", "passwd")

        with pytest.raises(TypeError):
            SSHConnectionManager("10.0.0.5", "root", password="passwd")

    def test_signature_has_no_password(self):
        params = inspect.signature(SSHConnectionManager.__init__).parameters
        assert "password" not in params
        # the key passphrase is a different thing and must still be there
        assert "private_key_password" in params

    def test_connect_never_sends_a_password(self):
        manager = SSHConnectionManager(
            "10.0.0.5", "root", look_for_keys=True, private_key_file_path="/tmp/key"
        )
        with patch.object(manager, "_SSHConnectionManager__client") as client:
            manager.get_client()

        client.connect.assert_called_once()
        kwargs = client.connect.call_args.kwargs
        assert "password" not in kwargs
        assert kwargs["key_filename"] == ["/tmp/key"]
        assert kwargs["allow_agent"] is False
        assert kwargs["username"] == "root"

    def test_connect_without_key_path_falls_back_to_default_keys(self):
        manager = SSHConnectionManager("10.0.0.5", "cephuser", look_for_keys=True)
        with patch.object(manager, "_SSHConnectionManager__client") as client:
            manager.get_client()

        kwargs = client.connect.call_args.kwargs
        assert "password" not in kwargs
        assert kwargs["look_for_keys"] is True

    def test_auth_failure_hint_mentions_missing_key(self):
        manager = SSHConnectionManager("10.0.0.5", "root", outage_timeout=1)
        with patch.object(manager, "_SSHConnectionManager__client") as client, patch(
            "ceph.ceph.sleep"
        ):
            client.connect.side_effect = Exception("Authentication failed")
            with pytest.raises(AssertionError) as err:
                manager.get_client()
        assert "password auth is not supported" in str(err.value)


class TestCephNode:
    def test_has_no_password_attributes(self):
        with patch("ceph.ceph.SSHConnectionManager"):
            node = CephNode(**_node_kwargs())

        for attr in ("password", "root_passwd", "root_login"):
            assert not hasattr(node, attr), f"{attr} should be gone"

    def test_builds_both_connections_without_a_password(self):
        with patch("ceph.ceph.SSHConnectionManager") as manager:
            CephNode(**_node_kwargs())

        assert manager.call_count == 2
        users = []
        for call in manager.call_args_list:
            assert len(call.args) == 2, "only (ip, username) may be positional"
            assert "password" not in call.kwargs
            users.append(call.args[1])
            assert call.kwargs["look_for_keys"] is True
            assert call.kwargs["private_key_file_path"] == "/tmp/does-not-exist-key"
        assert users == ["root", "cephuser"]

    def test_look_for_key_is_forced_on(self):
        """Even an explicit False must not disable key auth."""
        with patch("ceph.ceph.SSHConnectionManager"):
            node = CephNode(**_node_kwargs(look_for_key=False))
        assert node.look_for_key is True

    def test_unpickling_restores_key_based_connections(self):
        with patch("ceph.ceph.SSHConnectionManager"):
            node = CephNode(**_node_kwargs())
            state = node.__getstate__()

        assert "root_connection" not in state
        assert "connection" not in state

        restored = CephNode.__new__(CephNode)
        with patch("ceph.ceph.SSHConnectionManager") as manager:
            restored.__setstate__(state)

        assert manager.call_count == 2
        for call in manager.call_args_list:
            assert "password" not in call.kwargs
        assert manager.call_args_list[0].args[1] == "root"
        assert manager.call_args_list[1].args[1] == "cephuser"

    def test_connect_does_not_rotate_passwords(self):
        """connect() used to chpasswd root and cephuser. It must not now."""
        source = inspect.getsource(CephNode.connect)
        assert "chpasswd" not in source


class TestAuthorizeKey:
    def test_appends_key_idempotently(self):
        from cli.utilities.configure import authorize_key

        node = MagicMock()
        node.hostname = "node1"
        authorize_key(node, "ssh-rsa AAAAB3Nza test@host\n", sudo=True)

        cmds = [call.kwargs["cmd"] for call in node.exec_command.call_args_list]
        assert all(
            call.kwargs["sudo"] is True for call in node.exec_command.call_args_list
        )
        assert any("mkdir -p ~/.ssh" in c for c in cmds)
        # idempotent append, never a truncating write
        append = [c for c in cmds if "authorized_keys" in c and "grep -qxF" in c]
        assert len(append) == 1
        assert ">>" in append[0]
        assert "> ~/.ssh/authorized_keys" not in append[0].replace(">>", "")
        assert "sshpass" not in " ".join(cmds)

    def test_rejects_empty_key(self):
        from cli.exceptions import OperationFailedError
        from cli.utilities.configure import authorize_key

        node = MagicMock()
        node.hostname = "node1"
        with pytest.raises(OperationFailedError):
            authorize_key(node, "   ")


class TestSetupSshKeysDoesNotLockUsOut:
    """rm -rf ~/.ssh would delete the authorized_keys we log in with."""

    @pytest.mark.parametrize(
        "module",
        ["cli.utilities.configure", "cephci.utils.configure"],
    )
    def test_never_removes_the_ssh_directory(self, module):
        import importlib

        mod = importlib.import_module(module)
        source = inspect.getsource(mod.setup_ssh_keys)
        assert "rm -rf" not in source
        assert "SSHPASS" not in source
        assert "sshpass" not in source.lower().replace(
            "sshpass/ssh-copy-id", ""
        ), "sshpass must not be used"
        assert "authorize_key" in source

    @pytest.mark.parametrize(
        "module",
        ["cli.utilities.configure", "cephci.utils.configure"],
    )
    def test_remove_keys_targets_only_the_keypair(self, module):
        import importlib

        mod = importlib.import_module(module)
        assert mod.SSH_REMOVE_KEYS == "rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub"


class TestInventoryAuthorizedKeys:
    """Every Linux cloud-init file grants exactly one key per account: the
    CI service key for cephuser and root, and one personal key each for the
    operator accounts. Nothing else is authorized anywhere."""

    CI_KEY = (
        "ecdsa-sha2-nistp521 AAAAE2VjZHNhLXNoYTItbmlzdHA1MjEAAAAIbmlzdHA1MjEAAACFBA"
        "GAhrLVC1yr8ZP7pX1h8ghCIE7OAMpA6aXxGJkkYu8GPwJvmR5gl9CqJ4bcMc1gxVzJCXe0Aw9x"
        "rP3ahnWjMXkRJABWgN1jjv7lZkhFMfDgXhalYNzli7QIeOnujQaaHtWD/o6/Nky+P7fdwn37ev"
        "r5A0RslI6mxzn4es4PdHGTOjkDXQ== ibm-storage-ceph-ci-user"
    )
    PSATHYAN_KEY = (
        "ecdsa-sha2-nistp384 AAAAE2VjZHNhLXNoYTItbmlzdHAzODQAAAAIbmlzdHAzODQAAABhBP"
        "DlMqoFJqzj9yP5o3w4O/rm4fHWSDKAsDr4YxHHFN35hEQ5vb4xALP0KoZg3UthbqnbXWvtSD3V"
        "oG3EFvPLjXNoO8xnM6VZrrDZYw8MT9Em2i+mCam+tpDBWbOojGvEfw== psathyan-fips"
    )
    RCHILKUN_KEY = (
        "ecdsa-sha2-nistp521 AAAAE2VjZHNhLXNoYTItbmlzdHA1MjEAAAAIbmlzdHA1MjEAAACFBA"
        "HzT2JgaOA+RQsExHsk3E4IZjD4ZPNJghZbt4TucWcVd+jVotfWDFR+AHQwShYaCy4RCbXiPgWv"
        "ZUjMmRWwmZqOwwHRQ520hhwdc6+R/h7i7axVBO++kQDvvH450B+Vr9UHwVB9LICPBSXHXIUUdF"
        "B5eSUi8C5LSsxd+JnRIyhM9vOCcg== rakeshnarayanchilkunda@Rakeshs-MacBook-Pro-"
        "3.local"
    )
    # account name -> the single key it may authorize
    EXPECTED = {
        "cephuser": CI_KEY,
        "root": CI_KEY,
        "psathyan": PSATHYAN_KEY,
        "rchilkun": RCHILKUN_KEY,
    }
    # operator accounts that must have passwordless sudo
    OPERATORS = ("psathyan", "rchilkun")
    # Windows is an explicit exemption: not a Ceph node, no root account.
    EXEMPT = {"ibm-windows-server-2022-bx2-2x8.yaml"}

    def _inventories(self):
        import glob
        import os

        root = os.path.join(os.path.dirname(__file__), "..", "..", "conf", "inventory")
        return sorted(glob.glob(os.path.join(root, "*.yaml")))

    @pytest.mark.parametrize("account", ["cephuser", "root", "psathyan", "rchilkun"])
    def test_account_trusts_exactly_one_key(self, account):
        import os

        import yaml

        checked = 0
        for path in self._inventories():
            if os.path.basename(path) in self.EXEMPT:
                continue
            setup = (
                (yaml.safe_load(open(path).read()) or {})
                .get("instance", {})
                .get("setup")
            )
            if not setup:
                continue
            name = os.path.basename(path)
            users = (yaml.safe_load(setup) or {}).get("users") or []
            match = [
                u for u in users if isinstance(u, dict) and u.get("name") == account
            ]
            assert match, f"{name}: no {account} user"
            keys = (
                match[0].get("ssh-authorized-keys")
                or match[0].get("ssh_authorized_keys")
                or []
            )
            expected = self.EXPECTED[account]
            assert keys == [
                expected
            ], f"{name}: {account} must trust only its one key, got {keys}"
            checked += 1
        assert checked > 70, f"expected to check most inventories, checked {checked}"

    def test_no_stray_public_keys_remain(self):
        """No developer or jenkins keys left anywhere in the cloud-init."""
        import os
        import re

        allowed = set(self.EXPECTED.values())
        pattern = re.compile(r"(?m)^\s*-\s+(?:ssh-rsa|ssh-ed25519|ecdsa-\S+)\s+\S+.*$")
        for path in self._inventories():
            if os.path.basename(path) in self.EXEMPT:
                continue
            for line in pattern.findall(open(path).read()):
                found = line.strip().lstrip("-").strip()
                assert (
                    found in allowed
                ), f"{os.path.basename(path)}: stray key {found[:60]}..."

    def test_no_password_auth_in_cloud_init(self):
        import os

        import yaml

        for path in self._inventories():
            if os.path.basename(path) in self.EXEMPT:
                continue
            raw = open(path).read()
            setup = (yaml.safe_load(raw) or {}).get("instance", {}).get("setup")
            if not setup:
                continue
            cc = yaml.safe_load(setup)
            name = os.path.basename(path)
            assert "chpasswd" not in cc, f"{name}: chpasswd present"
            assert cc.get("ssh_pwauth") is not True, f"{name}: ssh_pwauth enabled"
            assert cc.get("disable_root") is not True, f"{name}: root login disabled"
            assert "PermitRootLogin yes" not in setup, f"{name}: PermitRootLogin yes"

    @pytest.mark.parametrize("account", ["cephuser"] + list(OPERATORS))
    def test_account_has_a_declared_primary_group(self, account):
        """cloud-init must create the group before assigning it as primary."""
        import os

        import yaml

        for path in self._inventories():
            if os.path.basename(path) in self.EXEMPT:
                continue
            setup = (
                (yaml.safe_load(open(path).read()) or {})
                .get("instance", {})
                .get("setup")
            )
            if not setup:
                continue
            name = os.path.basename(path)
            cc = yaml.safe_load(setup) or {}
            users = {
                u["name"]: u for u in (cc.get("users") or []) if isinstance(u, dict)
            }
            assert account in users, f"{name}: no {account} user"
            group = users[account].get("primary-group") or users[account].get(
                "primary_group"
            )
            assert group == account, f"{name}: {account} primary-group={group!r}"
            groups = cc.get("groups") or []
            assert group in groups, f"{name}: group {group!r} not in groups: {groups}"
            assert len(groups) == len(set(groups)), f"{name}: duplicate groups {groups}"

    @pytest.mark.parametrize("account", OPERATORS)
    def test_operator_has_passwordless_sudo(self, account):
        import os

        import yaml

        for path in self._inventories():
            if os.path.basename(path) in self.EXEMPT:
                continue
            setup = (
                (yaml.safe_load(open(path).read()) or {})
                .get("instance", {})
                .get("setup")
            )
            if not setup:
                continue
            name = os.path.basename(path)
            users = (yaml.safe_load(setup) or {}).get("users") or []
            match = [
                u for u in users if isinstance(u, dict) and u.get("name") == account
            ]
            assert match, f"{name}: no {account} user"
            sudo = str(match[0].get("sudo", ""))
            assert "NOPASSWD:ALL" in sudo, f"{name}: {account} sudo={sudo!r}"


class TestWindowsIsolation:
    """Windows still needs a password, but only via its own class."""

    def test_windows_manager_is_separate_from_ceph_manager(self):
        from cli.utilities.windows_utils import WindowsSSHConnectionManager

        assert WindowsSSHConnectionManager is not SSHConnectionManager
        params = inspect.signature(WindowsSSHConnectionManager.__init__).parameters
        assert "password" in params

    def test_windows_node_uses_password_connection(self):
        from cli.utilities.windows_utils import (
            WindowsCephNode,
            WindowsSSHConnectionManager,
        )

        with patch("ceph.ceph.SSHConnectionManager"), patch(
            "cli.utilities.windows_utils.WindowsSSHConnectionManager"
        ) as win_manager:
            node = WindowsCephNode(
                **_node_kwargs(username="Administrator", password="CephUser123")
            )

        win_manager.assert_called_once_with("10.0.0.5", "Administrator", "CephUser123")
        # both handles resolve to the single Windows session
        assert node.ssh == node.rssh
        assert WindowsSSHConnectionManager is not None
