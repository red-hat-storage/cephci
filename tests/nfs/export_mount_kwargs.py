"""Pure helpers for splitting export vs mount kwargs in NFS export/mount flows."""

# kwargs accepted by Mount.nfs(); export-only keys (e.g. enctag) must stay out.
MOUNT_OPTS_KEYS = frozenset(
    (
        "xprtsec",
        "proto",
        "extra_mount_options",
        "mounttimeout",
        "timeo",
        "retrans",
        "timeout",
    )
)

# Upgrade-only kwargs; stripped before export.create() and never forwarded to mount.
UPGRADE_KWARGS_KEYS = frozenset(
    (
        "installer_node",
        "during_upgrade",
        "nfs_wait_timeout",
        "mount_timeout",
        "mount_tries",
    )
)


def mount_opts_from_kwargs(kwargs, chown_cephuser=False):
    """Return mount kwargs without export-only or upgrade-only keys."""
    mount_opts = {k: v for k, v in kwargs.items() if k in MOUNT_OPTS_KEYS}
    mount_opts["chown_cephuser"] = chown_cephuser
    return mount_opts


def pop_upgrade_kwargs(kwargs):
    """Remove upgrade-scenario kwargs; remaining kwargs are for export.create()."""
    return {
        "installer_node": kwargs.pop("installer_node", None),
        "during_upgrade": kwargs.pop("during_upgrade", False),
        "nfs_wait_timeout": kwargs.pop("nfs_wait_timeout", 300),
        "mount_timeout": kwargs.pop("mount_timeout", 120),
        "mount_tries": kwargs.pop("mount_tries", 2),
    }
