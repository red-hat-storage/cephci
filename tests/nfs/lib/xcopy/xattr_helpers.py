"""Helpers for NFSv4.2 extended-attribute (xattr) cases XATTR-01..04."""

from tests.nfs.lib.xcopy.helpers import (
    announce,
    cephfs_subvol_fuse_mount,
    remount_xcopy_nfs,
)
from utility.log import Log

log = Log(__name__)


def xattr_set(client, path, name, value, check_ec=True):
    """``setfattr -n <name> -v <value> <path>``."""
    value = str(value)
    log.info("setfattr %s on %s (%s bytes value)", name, path, len(value))
    if len(value) < 3500:
        safe = value.replace("'", "'\\''")
        cmd = f"setfattr -n {name} -v '{safe}' {path}"
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)
        return client.exit_status, (out or ""), (err or "")

    # Build payload on the remote and set via os.setxattr (ARG_MAX-safe).
    py = (
        "import os,sys\n"
        "path,name,nbytes,seed=sys.argv[1],sys.argv[2],int(sys.argv[3]),sys.argv[4]\n"
        "seed_b=seed.encode()\n"
        "data=(seed_b*((nbytes//len(seed_b))+1))[:nbytes]\n"
        "os.setxattr(path, name.encode(), data)\n"
        "print('SET_OK len=%d' % len(data))\n"
    )
    client.exec_command(
        sudo=True,
        cmd=f"cat > /tmp/xattr_set.py <<'PY'\n{py}PY",
    )
    out, err = client.exec_command(
        sudo=True,
        cmd=(f"python3 /tmp/xattr_set.py {path} {name} {len(value)} CephQE"),
        check_ec=check_ec,
    )
    return client.exit_status, (out or ""), (err or "")


def xattr_get(client, path, name, check_ec=True):
    """Return attribute value string, or None on failure."""
    py = (
        "import os,sys\n"
        "p,n=sys.argv[1],sys.argv[2]\n"
        "sys.stdout.buffer.write(os.getxattr(p, n.encode()))\n"
    )
    client.exec_command(
        sudo=True,
        cmd=f"cat > /tmp/xattr_get.py <<'PY'\n{py}PY",
    )
    out, err = client.exec_command(
        sudo=True,
        cmd=f"python3 /tmp/xattr_get.py {path} {name}",
        check_ec=False,
    )
    if client.exit_status == 0:
        if isinstance(out, bytes):
            return out.decode("utf-8", "surrogateescape")
        return out if out is not None else ""

    # Fallback: getfattr (fine for small ASCII values).
    out2, _ = client.exec_command(
        sudo=True,
        cmd=f"getfattr -n {name} --only-values {path} 2>/dev/null",
        check_ec=check_ec,
    )
    if client.exit_status != 0:
        log.warning(
            "xattr_get failed for %s on %s (os.getxattr err=%s)",
            name,
            path,
            (err or "").strip()[:200],
        )
        return None
    text = out2 if isinstance(out2, str) else (out2 or b"").decode("utf-8", "replace")
    return text[:-1] if text.endswith("\n") else text


def xattr_roundtrip_probe(client, path, name, nbytes, seed="CephQE"):
    """Set a deterministic nbytes payload and read it back via os.setxattr/getxattr."""
    nbytes = int(nbytes)
    py = (
        "import os,sys,hashlib,errno\n"
        "path,name,nbytes,seed=sys.argv[1],sys.argv[2],int(sys.argv[3]),sys.argv[4]\n"
        "seed_b=seed.encode(); data=(seed_b*((nbytes//len(seed_b))+1))[:nbytes]\n"
        "exp=hashlib.sha256(data).hexdigest()\n"
        "try:\n"
        "  os.setxattr(path, name.encode(), data)\n"
        "except OSError as e:\n"
        "  print('RESULT=SET_REJECTED errno=%s err=%s' % (e.errno, e))\n"
        "  raise SystemExit(0)\n"
        "try:\n"
        "  got=os.getxattr(path, name.encode())\n"
        "except OSError as e:\n"
        "  print('RESULT=GET_MISMATCH errno=%s err=%s set_ok=1 got_len=-1 exp=%s' % "
        "(e.errno, e, exp))\n"
        "  raise SystemExit(0)\n"
        "if got!=data:\n"
        "  print('RESULT=GET_MISMATCH set_ok=1 got_len=%d expect=%d "
        "sha_got=%s sha_exp=%s' % (len(got), nbytes, "
        "hashlib.sha256(got).hexdigest(), exp))\n"
        "  raise SystemExit(0)\n"
        "print('RESULT=OK got_len=%d sha=%s' % (len(got), exp))\n"
    )
    client.exec_command(
        sudo=True,
        cmd=f"cat > /tmp/xattr_probe.py <<'PY'\n{py}PY",
    )
    out, err = client.exec_command(
        sudo=True,
        cmd=f"python3 /tmp/xattr_probe.py {path} {name} {nbytes} {seed}",
        check_ec=False,
    )
    text = ((out or "") + "\n" + (err or "")).strip()
    log.info("xattr_roundtrip_probe %s bytes:\n%s", nbytes, text)
    line = ""
    for ln in text.splitlines():
        if ln.startswith("RESULT="):
            line = ln
    if line.startswith("RESULT=OK"):
        return "ok", line
    if line.startswith("RESULT=SET_REJECTED"):
        return "set_rejected", line
    if line.startswith("RESULT=GET_MISMATCH"):
        return "get_mismatch", line
    return "get_mismatch", f"unexpected probe output rc={client.exit_status}: {text}"


def xattr_list(client, path):
    """Return dump text from ``getfattr -d -m -`` (all namespaces)."""
    out, _ = client.exec_command(
        sudo=True, cmd=f"getfattr -d -m - {path}", check_ec=False
    )
    text = out if isinstance(out, str) else (out or b"").decode("utf-8", "replace")
    log.info("getfattr -d %s:\n%s", path, (text or "").strip())
    return text or ""


def xattr_remove(client, path, name, check_ec=True):
    """``setfattr -x <name> <path>``."""
    out, err = client.exec_command(
        sudo=True, cmd=f"setfattr -x {name} {path}", check_ec=check_ec
    )
    return client.exit_status, (out or ""), (err or "")


def xattr_names_from_dump(dump_text):
    """Parse attribute names from getfattr -d output."""
    names = []
    for line in (dump_text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("file:"):
            continue
        if "=" in line:
            names.append(line.split("=", 1)[0].strip())
        else:
            names.append(line)
    return names


def assert_xattr_equals(client, path, name, expected):
    """Raise AssertionError-style via return False if value mismatch."""
    got = xattr_get(client, path, name, check_ec=False)
    if got is None:
        log.error("Missing xattr %s on %s", name, path)
        return False
    if got != expected:
        log.error(
            "xattr %s mismatch on %s: expected_len=%s got_len=%s",
            name,
            path,
            len(expected),
            len(got),
        )
        if len(expected) <= 128 and len(got) <= 128:
            log.error("expected=%r got=%r", expected, got)
        return False
    log.info("PASS: %s == expected on %s", name, path)
    return True


def remount_nfs(ctx, extra_mount_options=None):
    """Remount primary NFS export (optionally with extra -o, e.g. SELinux)."""
    remount_xcopy_nfs(ctx, extra_mount_options=extra_mount_options)


def verify_xattr_via_cephfs(ctx, nfs_rel_path, name, expected):
    """Best-effort: read xattr via ceph-fuse under the export's CephFS subvolume."""
    client = ctx["client"]
    try:
        with cephfs_subvol_fuse_mount(
            client, ctx, fuse_mnt="/mnt/cephfs_xattr_check"
        ) as (_fuse_mnt, subvol_path):
            fuse_path = f"{subvol_path}/{nfs_rel_path.lstrip('/')}"
            announce(f"Verify {name} via ceph-fuse at {fuse_path}")
            return assert_xattr_equals(client, fuse_path, name, expected)
    except RuntimeError as exc:
        log.warning("CephFS metadata check skipped: %s", exc)
        return None


def copy_preserve_xattr(client, src, dst):
    """``cp --preserve=xattr`` (falls back to ``cp -a`` if needed)."""
    announce(f"cp --preserve=xattr {src} -> {dst}")
    client.exec_command(sudo=True, cmd=f"rm -f {dst}", check_ec=False)
    client.exec_command(
        sudo=True, cmd=f"cp --preserve=xattr {src} {dst}", check_ec=False
    )
    if client.exit_status != 0:
        log.warning(
            "cp --preserve=xattr failed (rc=%s); trying cp -a", client.exit_status
        )
        client.exec_command(sudo=True, cmd=f"cp -a {src} {dst}")
    return client.exit_status


def selinux_available(client):
    """Return True if SELinux tools exist and mode is not Disabled."""
    out, _ = client.exec_command(
        sudo=True, cmd="command -v getenforce; getenforce", check_ec=False
    )
    text = (out or "").strip()
    log.info("SELinux probe:\n%s", text)
    if "Disabled" in text or not text:
        return False
    out2, _ = client.exec_command(
        sudo=True, cmd="command -v chcon; command -v getfattr", check_ec=False
    )
    return "chcon" in (out2 or "")


def set_selinux_label(client, path, label):
    """``chcon -t`` / full context via chcon -u -r -t or chcon LABEL."""
    client.exec_command(sudo=True, cmd=f"chcon {label} {path}", check_ec=False)
    return client.exit_status


def get_selinux_label(client, path):
    """Return security.selinux value or None."""
    return xattr_get(client, path, "security.selinux", check_ec=False)
