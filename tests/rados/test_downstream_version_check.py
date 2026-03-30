"""RADOS tests for downstream Ceph version string in CLI output.

Validates that the value from ``/etc/ceph_version`` on the installer appears in
``ceph version`` and ``ceph versions`` output, and that formatted output
(plain, json, yaml, etc.) is structurally valid.
Existing Bug - https://ibm-ceph.atlassian.net/browse/IBMCEPH-13690

"""

import traceback

from packaging.version import InvalidVersion, Version

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
    - For each output format, run validation pipelines (e.g. jq, yaml, parse)
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

        # Due to BMCEPH-13690, the xml and xml-pretty options were removed from the formats variable,
        # and the corresponding verification lines for these formats were commented out in versions_formats.
        formats = ["plain", "json", "json-pretty", "yaml"]
        versions_formats = [
            "> /dev/null 2>&1; echo $?",
            "json | jq . > /dev/null 2>&1; echo $?",
            "json-pretty | jq . > /dev/null 2>&1; echo $?",
            'yaml | python3 -c "import sys,yaml;sys.exit(0 if yaml.safe_load(sys.stdin) else 1)"; echo $?',
            # 'xml | python3 -c "import sys,xml.etree.ElementTree as ET;ET.parse(sys.stdin);sys.exit(0)" '
            # "2>/dev/null; echo $?",
            # "xml-pretty | python3 -c "
            # '"import sys,xml.etree.ElementTree as ET;ET.parse(sys.stdin);sys.exit(0)" 2>/dev/null; echo $?',
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
            "Check 2: formatted output must be valid (plain, json, yaml  variants).",
        )

        for base_cmd in base_commands:
            for ver_fmt, fmt in zip(versions_formats, formats):
                if fmt == "plain":
                    full_cmd = f"{base_cmd} {ver_fmt}"
                else:
                    full_cmd = f"{base_cmd} -f {ver_fmt}"

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
    return 0


def is_exact_stable_version(expected: str, actual: str) -> bool:
    """
    Compare expected and actual versions and accept only exact stable matches.

    Args:
        expected: Downstream version expected from ``/etc/ceph_version``.
        actual: Version parsed from command output.

    Returns:
        True when ``actual`` parses successfully, is not a prerelease, and
        exactly matches ``expected``; otherwise False.
    """
    log.debug(
        "Validating stable version match (expected=%r, actual=%r).",
        expected,
        actual,
    )
    try:
        expected_version = Version(expected)
        actual_version = Version(actual)

        if actual_version.is_prerelease:
            log.info(
                "Version %r is a prerelease; treating as non-match.",
                actual,
            )
            return False

        is_match = actual_version == expected_version
        log.debug("Exact stable version comparison result: %s", is_match)
        return is_match
    except InvalidVersion:
        log.error(
            "Unable to parse version values for comparison (expected=%r, actual=%r).",
            expected,
            actual,
        )
        return False


def validate_downstream_version(base_cmd, output, down_stream_version):
    """
    Validate downstream version for ``ceph version`` and ``ceph versions`` output.

    Args:
        base_cmd: Base Ceph command under validation.
        output: Parsed output returned by ceph command execution.
        down_stream_version: Expected downstream version string.
    Returns:
        0 when all version checks pass, else 1.
    """
    log.info("Validating downstream version for command: %s", base_cmd)

    if base_cmd == "ceph version":
        log.info("ceph version output: %s", output)

        actual_version = extract_version(output.get("version", ""))
        log.info("Parsed version from ceph version output: %s", actual_version)
        if not is_exact_stable_version(down_stream_version, actual_version):
            log.error(
                "Expected downstream version %r, but found %r.",
                down_stream_version,
                actual_version,
            )
            return 1

        log.info("Downstream version %r validated successfully.", down_stream_version)

    elif base_cmd == "ceph versions":
        log.debug("Iterating daemon versions from ceph versions output.")
        for daemon_map in output.values():
            version_string = next(iter(daemon_map))
            actual_version = extract_version(version_string)
            log.info("Parsed daemon version: %s", actual_version)
            if not is_exact_stable_version(down_stream_version, actual_version):
                log.error(
                    "Expected %r, but daemon has %r (raw: %s).",
                    down_stream_version,
                    actual_version,
                    version_string,
                )
                return 1

            log.info(
                "Daemon version %r validated successfully (raw: %s).",
                actual_version,
                version_string,
            )
    else:
        log.error("Unsupported command for downstream version validation: %s", base_cmd)
        return 1

    return 0


def extract_version(text: str) -> str:
    """
    Extract the first valid version token from a text string.

    Args:
        text: Command output string containing a version value.

    Returns:
        First token that parses as a packaging.version ``Version``; empty string if none.
    """
    log.debug("Extracting version token from text: %r", text)

    for token in text.split():
        try:
            Version(token)
            log.debug("Extracting version token text: %s", token)
            return token
        except InvalidVersion:
            continue
    log.warning("No valid version token found in text: %r", text)
    return ""
