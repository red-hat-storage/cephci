"""Shared builders for SSH key authorization on cluster nodes.

The same idempotent, safely-quoted commands are needed from several execution
channels:

- ``ceph.ceph`` runs them over a raw paramiko client or a system ``ssh``
  subprocess, where ``sudo`` must be embedded in the command itself.
- ``cli.utilities.configure.authorize_key`` covers the framework's own node
  session flavour (``node.exec_command`` with framework-managed sudo).

Keep this module free of framework imports -- it is loaded by the low-level
``ceph.ceph`` module and must not create import cycles.
"""

import base64

CEPH_USER = "cephuser"


def key_blob(public_key):
    """Return the unique blob part of a public key line.

    The blob is safe to embed in a ``grep -F`` pattern, unlike the full line
    whose comment field may contain shell metacharacters.

    Args:
        public_key (str): Public key content (single line)

    Returns: the key blob (str)
    """
    fields = public_key.strip().split()
    return fields[1] if len(fields) > 1 else public_key.strip()


def key_b64(public_key):
    """Return the base64 encoding of the full public key line.

    Transporting the key as base64 sidesteps shell quoting entirely on
    channels that expand the command more than once.

    Args:
        public_key (str): Public key content (single line)

    Returns: base64 encoded key (str)
    """
    return base64.b64encode(public_key.strip().encode()).decode()


def build_user_setup_cmds(username=CEPH_USER):
    """Return commands ensuring ``username`` exists with passwordless sudo.

    Args:
        username (str): User to create (idempotent when it already exists)

    Returns: list of self-contained shell commands (sudo embedded)
    """
    return [
        f"id {username} >/dev/null 2>&1 || "
        f"(sudo groupadd -f {username} && "
        f"sudo useradd -m -s /bin/bash -g {username} {username})",
        f"echo '{username} ALL=(ALL) NOPASSWD:ALL' "
        f"| sudo tee /etc/sudoers.d/{username}",
        f"sudo chmod 440 /etc/sudoers.d/{username}",
    ]


def build_authorize_key_cmds(public_key, home, owner=None):
    """Return commands appending ``public_key`` to ``home``'s authorized_keys.

    The append is idempotent (the key blob is grepped for first) and never
    truncates existing entries -- removing the key the framework logged in
    with would lock it out of the node.

    Args:
        public_key (str): Public key content (single line)
        home (str): Home directory of the target account
        owner (str): chown the .ssh tree to this user when given

    Returns: list of self-contained shell commands (sudo embedded)
    """
    authorized_keys = f"{home}/.ssh/authorized_keys"
    cmds = [
        f"sudo mkdir -p {home}/.ssh && sudo chmod 700 {home}/.ssh",
        f"sudo touch {authorized_keys}",
        f"(sudo grep -qF '{key_blob(public_key)}' {authorized_keys} || "
        f"echo '{key_b64(public_key)}' | base64 -d | sudo tee -a {authorized_keys})",
        f"sudo chmod 600 {authorized_keys}",
    ]
    if owner:
        cmds.append(f"sudo chown -R {owner}:{owner} {home}/.ssh")
    return cmds
