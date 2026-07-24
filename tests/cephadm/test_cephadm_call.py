from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """Verify cephadm call with command with long output
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Upload the script "cephadm-call.py" to the installer node
    installer.upload_file(
        "cli/utilities/cephadm-call.py",
        "/root/cephadm-call.py",
        sudo=True,
    )

    # Execute script
    cmd = "python3 cephadm-call.py --timeout 30 cat test.txt > /dev/null"
    out = installer.exec_command(sudo=True, cmd=cmd)[0]
    if out:
        raise OperationFailedError(f"cephadm command failed with long output : {out}")
    return 0
