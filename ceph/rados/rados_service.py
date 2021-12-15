"""
   This module contains the methods to perform general ceph cluster modification operations.
    1. Remove OSD
    2. Add OSD
    3. Set osd out
    3. Zap device path
"""
import logging

from ceph.rados.core_workflows import RadosOrchestrator

log = logging.getLogger(__name__)


class RadosService(RadosOrchestrator):
    """
    Perform general cluster modification operations
    """

    def run_command(self, cmd: str):
        """
        Runs ceph commands  for the action specified
        Args:
            cmd: Command that needs to be run
        Returns:
            out (Str), err (Str) stdout and stderr response
        """
        try:
            out, err = self.node.shell([cmd])
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            return None
        return out, err

    def set_osd_devices_unamanged(self, unmanaged: bool) -> bool:
        """
        Sets osd device unmanaged as true/false
        Returns:
            Pass->true, Fail->false
        """
        out, _ = self.run_command(
            f"ceph orch apply osd --all-available-devices --unmanaged={unmanaged}"
        )
        if "Scheduled osd.all-available-devices update" in out:
            return True
        return False

    def set_osd_out(self, osd_id) -> bool:
        """
        Set osd out
        Args:
            osd_id: OSD id
        Returns:
            Pass->true, Fail->false
        """
        cmd = f"ceph osd out {osd_id}"
        out, err = self.run_command(cmd)
        if f"marked out osd.{osd_id}" in err:
            return True
        return False

    def remove_osd(self, osd_id) -> bool:
        """
        remove osd
        Args:
            osd_id: OSD id
        Returns:
            Pass->true, Fail->false
        """
        cmd = f"ceph orch osd rm {osd_id}"
        out, _ = self.run_command(cmd)
        if "Scheduled OSD(s) for removal" in out:
            return True
        return False

    def zap_device(self, host, device) -> bool:
        """
        zap device
        Args:
            host: hostname
            device: device path
        Returns:
            Pass->true, Fail->false
        """
        cmd = f"ceph orch device zap {host} {device} --force"
        out, _ = self.run_command(cmd)
        if not out:
            return True
        return False

    def add_osd(self, host, device, osd_id) -> bool:
        """
        add osd
        Args:
            host: hostname
            device: device path
            osd_id: osd id
        Returns:
            Pass->true, Fail->false
        """
        cmd = f"ceph orch daemon add osd {host}:{device}"
        out, _ = self.run_command(cmd)
        if f"Created osd(s) {osd_id} on host '{host}'" in out:
            return True
        return False
