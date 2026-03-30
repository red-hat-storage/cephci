"""RADOS tests for downstream Ceph version string in CLI output.

Validates that the value from ``/etc/ceph_version`` on the installer appears in
``ceph version`` and ``ceph versions`` output, and that formatted output
(plain, json, yaml, xml, etc.) is structurally valid.
"""

import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify downstream version suffix in ceph version commands and output formats.

    Steps:
    - Skip if Ceph release is below 9.1 (downstream suffix not applicable).
    - Read ``/etc/ceph_version`` from the installer node.
    - Assert the downstream string appears in ``ceph version`` and ``ceph versions``.
    - For each output format, run validation pipelines (e.g. jq, yaml, xml parse)
      and assert exit status is zero.

    Returns:
        0 on success, 1 on validation or execution failure.
    """
    log.debug("Test workflow started.")
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    rhbuild = config.get("rhbuild")
    if not rhbuild:
        log.error(
            "config.rhbuild is missing; cannot determine Ceph version for this test."
        )
        return 1

    try:
        ceph_version = float(rhbuild.split("-")[0])
        if ceph_version < 9.1:
            log.info(
                "NOTE: Downstream version suffix is supported from Ceph release 9.1 onwards; "
                "skipping validation for earlier versions (current parsed version: %s).",
                ceph_version,
            )
            return 0

        log.info(
            "Ceph version from rhbuild is %s; running downstream version and format checks.",
            ceph_version,
        )

        formats = ["plain", "json", "json-pretty", "yaml", "xml", "xml-pretty"]
        versions_formats = [
            "plain",
            "json | jq . > /dev/null 2>&1; echo $?",
            "json-pretty | jq . > /dev/null 2>&1; echo $?",
            'yaml | python3 -c "import sys,yaml;sys.exit(0 if yaml.safe_load(sys.stdin) else 1)"; echo $',
            'xml | python3 -c "import sys,xml.etree.ElementTree as ET;ET.parse(sys.stdin);sys.exit(0)" '
            "2>/dev/null; echo $?",
            "xml-pretty | python3 -c "
            '"import sys,xml.etree.ElementTree as ET;ET.parse(sys.stdin);sys.exit(0)" 2>/dev/null; echo $?',
        ]

        base_commands = ["ceph version", "ceph versions"]
        installer = ceph_cluster.get_nodes(role="installer")[0]
        cmd_cat = "cat /etc/ceph_version"
        log.info(
            "Reading downstream version from installer %s via: %s",
            installer.hostname,
            cmd_cat,
        )
        content, _ = rados_object.node.shell([cmd_cat], pretty_print=True)
        down_stream_version = content.strip()

        log.info(
            "/etc/ceph_version on installer %s (%s): %s",
            installer.hostname,
            installer.ip_address,
            down_stream_version,
        )

        log.info(
            "Check 1: downstream version string must appear in ceph version / ceph versions output."
        )
        for cmd in base_commands:
            log.debug("Running command: %s", cmd)
            cmd_output = rados_object.run_ceph_command(cmd)
            if validate_downstream_version(cmd, cmd_output, down_stream_version):
                log.error(
                    "Downstream version %r missing or invalid for command %r.",
                    down_stream_version,
                    cmd,
                )
                return 1
            log.info(
                "Downstream version %r found in %r output.", down_stream_version, cmd
            )

        log.info(
            "Check 2: formatted output must be valid (plain, json, yaml, xml variants).",
        )
        for base_cmd in base_commands:
            for ver_fmt, fmt in zip(versions_formats, formats):
                full_cmd = (
                    base_cmd if ver_fmt == "plain" else f"{base_cmd} -f {ver_fmt}"
                )
                base_fmt = fmt.split("-")[0]
                try:
                    log.info("Executing format check (%s): %s", base_fmt, full_cmd)
                    output, _ = rados_object.client.exec_command(
                        sudo=True,
                        cmd=full_cmd,
                    )
                    if int(output) != 0:
                        log.error(
                            "Format validation failed for %s (exit code %s); command: %s",
                            base_fmt,
                            output,
                            full_cmd,
                        )
                        return 1
                    log.info(
                        "Format %s validated successfully (exit code %s).",
                        base_fmt,
                        output,
                    )

                except Exception as e:
                    log.error(
                        "Command execution failed for format %s (base command %s): %s",
                        fmt,
                        base_cmd,
                        e,
                    )
                    return 1

        log.info("All downstream version and format checks passed.")
    except Exception as e:
        log.error("Test failed with unexpected error: %s", e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n\n================ Execution of finally block =======================\n\n"
        )
    return 0


def validate_downstream_version(base_cmd, output, down_stream_version):
    """
    For ``ceph version``, checks the string in the text output. For ``ceph versions``,
    checks each daemon's version string in the parsed dict.

    Args:
        base_cmd: Either ``ceph version`` or ``ceph versions``.
        output: Raw string for ``ceph version``, or dict for ``ceph versions``.
        down_stream_version: Expected substring from ``/etc/ceph_version``.

    Returns:
        0 if validation passes, 1 if the downstream string is missing anywhere.
    """
    if base_cmd == "ceph version":
        log.debug(
            "Validating ceph version output (length %s).", len(output) if output else 0
        )
        log.info("ceph version output: %s", output)
        if down_stream_version not in output:
            log.error(
                "Downstream version %r not found in ceph version output.",
                down_stream_version,
            )
            return 1
        log.info(
            "Downstream version %r present in ceph version output.", down_stream_version
        )
    elif base_cmd == "ceph versions":
        for daemon_map in output.values():
            version_string = next(iter(daemon_map))
            if down_stream_version not in version_string:
                log.error(
                    "Downstream version %r not in daemon map %s (version string: %s).",
                    down_stream_version,
                    daemon_map,
                    version_string,
                )
                return 1
            log.info(
                "Downstream version %r present in daemon map %s (version string: %s).",
                down_stream_version,
                daemon_map,
                version_string,
            )
    return 0
