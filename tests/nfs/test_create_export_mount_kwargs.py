"""Unit tests for export vs mount kwargs routing in NFS export/mount flows."""

from tests.nfs.export_mount_kwargs import (
    MOUNT_OPTS_KEYS,
    UPGRADE_KWARGS_KEYS,
    mount_opts_from_kwargs,
    pop_upgrade_kwargs,
)


def test_byok_enctag_stays_export_only():
    """BYOK callers pass enctag for export.create; mount must not receive it."""
    export_kwargs = {"enctag": "uuid-for-byok-key"}
    mount_opts = mount_opts_from_kwargs(export_kwargs)

    assert export_kwargs == {"enctag": "uuid-for-byok-key"}
    assert mount_opts == {"chown_cephuser": False}


def test_tls_xprtsec_forwarded_to_mount():
    """TLS tests pass xprtsec for both export.create and mount -o xprtsec=tls."""
    export_kwargs = {"xprtsec": "tls"}
    mount_opts = mount_opts_from_kwargs(export_kwargs)

    assert export_kwargs == {"xprtsec": "tls"}
    assert mount_opts == {"xprtsec": "tls", "chown_cephuser": False}


def test_upgrade_kwargs_removed_before_export_create():
    """Upgrade tuning kwargs must not leak into export.create or mount."""
    export_kwargs = {
        "enctag": "uuid",
        "installer_node": "node1",
        "during_upgrade": True,
        "nfs_wait_timeout": 120,
        "mount_timeout": 60,
        "mount_tries": 1,
    }
    upgrade_kwargs = pop_upgrade_kwargs(export_kwargs)
    mount_opts = mount_opts_from_kwargs(export_kwargs)

    assert export_kwargs == {"enctag": "uuid"}
    assert upgrade_kwargs["during_upgrade"] is True
    assert upgrade_kwargs["installer_node"] == "node1"
    assert mount_opts == {"chown_cephuser": False}


def test_chown_cephuser_explicit_param():
    export_kwargs = {"xprtsec": "tls"}
    mount_opts = mount_opts_from_kwargs(export_kwargs, chown_cephuser=True)

    assert mount_opts == {"xprtsec": "tls", "chown_cephuser": True}


def test_mount_opts_keys_match_mount_nfs_surface():
    """Keep MOUNT_OPTS_KEYS aligned with cli.utilities.filesys.Mount.nfs kwargs."""
    assert MOUNT_OPTS_KEYS == {
        "xprtsec",
        "proto",
        "extra_mount_options",
        "mounttimeout",
        "timeo",
        "retrans",
        "timeout",
    }
    assert UPGRADE_KWARGS_KEYS.isdisjoint(MOUNT_OPTS_KEYS)


if __name__ == "__main__":
    test_byok_enctag_stays_export_only()
    test_tls_xprtsec_forwarded_to_mount()
    test_upgrade_kwargs_removed_before_export_create()
    test_chown_cephuser_explicit_param()
    test_mount_opts_keys_match_mount_nfs_surface()
    print("All kwargs routing tests passed")
