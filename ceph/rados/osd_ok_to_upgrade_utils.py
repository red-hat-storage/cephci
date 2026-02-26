"""
Test module for functional tests of the ceph osd ok-to-upgrade feature.

The command under test:
    ceph osd ok-to-upgrade <crush_bucket> <ceph_version> [max]

Parameters:
    crush_bucket: Valid CRUSH bucket of type osd, host, chassis, or rack.
    ceph_version: Valid short ceph version (e.g. 20.2.0-abc).
    max: (Optional) Maximum number of OSDs to select for upgrade.
"""

from dataclasses import dataclass

from ceph.ceph import CommandFailed
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


@dataclass(frozen=True)
class OsdOkToUpgradeCommandOutput:
    """
    Immutable result of a successful 'ceph osd ok-to-upgrade' command.

    Attributes:
        ok_to_upgrade: Whether it is safe to upgrade OSDs in the bucket.
        all_osds_upgraded: Whether all OSDs in the bucket are already at target version.
        osds_in_crush_bucket: OSD IDs in the given CRUSH bucket.
        osds_ok_to_upgrade: OSD IDs that are safe to upgrade.
        osds_upgraded: OSD IDs already at the target version.
        bad_no_version: OSD IDs that could not report version (bad or no version).
    """

    ok_to_upgrade: bool
    all_osds_upgraded: bool
    osds_in_crush_bucket: list
    osds_ok_to_upgrade: list
    osds_upgraded: list
    bad_no_version: list

    def __str__(self):
        """Return a human-readable summary of the command output."""
        return (
            f"ok_to_upgrade: {self.ok_to_upgrade}\n"
            f"all_osds_upgraded: {self.all_osds_upgraded}\n"
            f"osds_in_crush_bucket: {self.osds_in_crush_bucket}\n"
            f"osds_ok_to_upgrade: {self.osds_ok_to_upgrade}\n"
            f"osds_upgraded: {self.osds_upgraded}\n"
            f"bad_no_version: {self.bad_no_version}\n"
        )

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __eq__(self, other):
        if not isinstance(other, OsdOkToUpgradeCommandOutput):
            return NotImplemented

        log.info("\nComparing expected and actual objects...")
        log.info(f"\nExpected:\n{self}")
        log.info(f"\nActual:\n{other}")

        # Compare simple fields first
        if self.ok_to_upgrade != other.ok_to_upgrade:
            log.warning("Mismatch: ok_to_upgrade")
            return False

        if self.all_osds_upgraded != other.all_osds_upgraded:
            log.warning("Mismatch: all_osds_upgraded")
            return False

        # Compare list fields
        list_fields = [
            "osds_in_crush_bucket",
            "osds_ok_to_upgrade",
            "osds_upgraded",
            "bad_no_version",
        ]
        for field in list_fields:
            self_val = getattr(self, field)
            other_val = getattr(other, field)
            self_val.sort()
            other_val.sort()
            if self_val != other_val:
                log.warning(
                    f"Mismatch in {field}: expected={self_val}, actual={other_val}"
                )
                return False

        # All fields matched
        return True


class OsdOkToUpgradeCommand:
    """
    Builder for 'ceph osd ok-to-upgrade' with optional max limit.

    Args:
        crush_bucket: CRUSH bucket name (e.g. rack1, host1).
        ceph_version: Target ceph version (e.g. 20.2.0-abc).
        rados_obj: RadosOrchestrator instance used to run the command.

    Example:
        output = OsdOkToUpgradeCommand("rack1", "20.2.0-abc", rados_obj).execute()
        output = OsdOkToUpgradeCommand("rack1", "20.2.0-abc", rados_obj).add_max(30).execute()

    Returns:
        OsdOkToUpgradeCommandOutput with ok_to_upgrade, all_osds_upgraded,
        osds_in_crush_bucket, osds_ok_to_upgrade, osds_upgraded, bad_no_version.
    """

    def __init__(
        self,
        crush_bucket: str,
        ceph_version: str,
        rados_obj: RadosOrchestrator,
        max: str = "",
    ):
        self.command = f"ceph osd ok-to-upgrade {crush_bucket} {ceph_version} {max}"
        self.rados_obj: RadosOrchestrator = rados_obj
        log.debug(
            "OsdOkToUpgradeCommand initialized: crush_bucket=%s, ceph_version=%s",
            crush_bucket,
            ceph_version,
        )

    def execute(self):
        """
        Run the ok-to-upgrade command and return parsed output.

        Returns:
            OsdOkToUpgradeCommandOutput with command result fields.

        Raises:
            CommandFailed: If the command fails or returns stderr.
        """
        log.info("Executing: %s", self.command)
        out, err = self.rados_obj.run_ceph_command(
            cmd=self.command, print_output=True, client_exec=True, return_err=True
        )
        if len(err) > 0:
            log.error("ok-to-upgrade command failed. err: %s", err)
            raise CommandFailed(f"Command execution failed. err: {err}")

        result = OsdOkToUpgradeCommandOutput(
            ok_to_upgrade=out["ok_to_upgrade"],
            all_osds_upgraded=out["all_osds_upgraded"],
            osds_in_crush_bucket=out["osds_in_crush_bucket"],
            osds_upgraded=out["osds_upgraded"],
            osds_ok_to_upgrade=out["osds_ok_to_upgrade"],
            bad_no_version=out["bad_no_version"],
        )
        log.info(
            "ok-to-upgrade succeeded: ok_to_upgrade=%s, all_osds_upgraded=%s, osds_ok_to_upgrade=%s",
            result.ok_to_upgrade,
            result.all_osds_upgraded,
            result.osds_ok_to_upgrade,
        )
        return result


def execute_negative_scenario(
    command: OsdOkToUpgradeCommand, expected_err_substring: str
) -> None:
    """
    Run a command that is expected to fail and verify the error message.

    Args:
        command: The OsdOkToUpgradeCommand to run (expected to raise CommandFailed).
        expected_err_substring: String that must appear in the raised error message.

    Raises:
        Exception: If the command succeeds, or if the error does not contain
            expected_err_substring, or if an unexpected exception type is raised.
    """
    log.info(
        "Running negative scenario: command=%s, expected_err_substring=%s",
        command.command,
        expected_err_substring,
    )
    try:
        _ = command.execute()
        log.error("Command should have failed but succeeded")
        assert False, "Command should have failed"
    except CommandFailed as e:
        if expected_err_substring in str(e):
            log.info("Expected error received: %s", str(e))
        else:
            log.error(
                "Unexpected error message: %s (expected substring: %s)",
                e,
                expected_err_substring,
            )
            raise Exception(e) from e
    except Exception as e:
        log.error("Unexpected exception in negative scenario: %s", e)
        raise Exception(e) from e
