"""VIP mount, basic IO validation, and export cleanup on clients."""

from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.utils import perform_lookups
from tests.nfs.lib.multi_active.constants import log_section
from utility.log import Log

log = Log(__name__)
from tests.nfs.lib.multi_active.client_io import NfsMultiActiveClientIO
from tests.nfs.nfs_operations import mount_cleanup_retry, mount_retry


class NfsMultiActiveClient(NfsMultiActiveClientIO):
    """VIP mount, minimal IO validation, unmount, and background IO (via mixin)."""

    @staticmethod
    def client_mount_ip(client):
        ip = getattr(client, "ip_address", None)
        if not ip:
            raise OperationFailedError(
                f"Could not determine mount IP for client {client.hostname}"
            )
        return ip

    @staticmethod
    def mount_via_vip(client, vip, nfs_mount, export_name, port, version="4.2"):
        mount_server = vip.split("/")[0]
        log.info(
            "Mounting %s on %s via VIP %s (client IP %s)",
            export_name,
            client.hostname,
            vip,
            NfsMultiActiveClient.client_mount_ip(client),
        )
        client.create_dirs(dir_path=nfs_mount, sudo=True)
        mount_retry(client, nfs_mount, version, port, mount_server, export_name)
        client.exec_command(sudo=True, cmd=f"chown cephuser:cephuser {nfs_mount}")
        sleep(2)

    @staticmethod
    def _umount_lazy_then_force(client, nfs_mount):
        """Try lazy unmount first; fall back to force unmount on failure."""
        log.info("Lazy-unmounting NFS on %s: %s", client.hostname, nfs_mount)
        _, err = client.exec_command(
            sudo=True,
            cmd=f"umount -l {nfs_mount}",
            check_ec=False,
        )
        if err:
            log.warning(
                "Lazy umount %s on %s failed (%s); trying force unmount",
                nfs_mount,
                client.hostname,
                err.strip(),
            )
            log.info("Force-unmounting NFS on %s: %s", client.hostname, nfs_mount)
            _, err = client.exec_command(
                sudo=True,
                cmd=f"umount -f {nfs_mount}",
                check_ec=False,
            )
        return err

    @staticmethod
    def remount_clients_via_vip(
        clients, vip, nfs_mount, export_name, port, version="4"
    ):
        """Force-remount clients on the VIP to refresh stick-table pins."""
        for client in clients:
            log.info("Remounting %s on %s via VIP %s", nfs_mount, client.hostname, vip)
            err = NfsMultiActiveClient._umount_lazy_then_force(client, nfs_mount)
            if err:
                log.warning(
                    "umount %s on %s before remount: %s",
                    nfs_mount,
                    client.hostname,
                    err.strip(),
                )
            NfsMultiActiveClient.mount_via_vip(
                client, vip, nfs_mount, export_name, str(port), version
            )

    @staticmethod
    def run_basic_io(
        writer_client,
        reader_client,
        nfs_mount,
        file_count=2,
        io_subdir="io_check",
    ):
        """Exercise minimal create/write, cross-client read/list, append, and cleanup."""
        io_dir = f"{nfs_mount.rstrip('/')}/{io_subdir}"
        test_file = f"{io_dir}/file1"
        log_section(log, "NFS MULTI-ACTIVE BASIC IO")
        log.info(
            "IO dir %s — writer %s, reader %s, file_count=%s",
            io_dir,
            writer_client.hostname,
            reader_client.hostname,
            file_count,
        )

        writer_client.exec_command(sudo=True, cmd=f"mkdir -p {io_dir}")
        writer_client.exec_command(sudo=True, cmd=f"chown cephuser:cephuser {io_dir}")
        NfsMultiActiveClient._assert_io_dir_on_nfs(writer_client, io_dir)
        NfsMultiActiveClient._assert_io_dir_on_nfs(reader_client, io_dir)

        for i in range(1, file_count + 1):
            writer_client.exec_command(
                sudo=True,
                cmd=f"touch {io_dir}/file{i} && echo write-test-{i} > {io_dir}/file{i}",
            )

        perform_lookups(reader_client, io_dir, file_count)

        out, _ = reader_client.exec_command(sudo=True, cmd=f"cat {test_file}")
        if "write-test-1" not in (out or ""):
            raise OperationFailedError(
                f"Cross-client read failed on {reader_client.hostname}"
            )

        reader_client.exec_command(sudo=True, cmd=f"mkdir -p {io_dir}/subdir")
        writer_client.exec_command(
            sudo=True,
            cmd=f"echo nfs-multi-active-io >> {test_file}",
        )
        out, _ = reader_client.exec_command(sudo=True, cmd=f"cat {test_file}")
        if "nfs-multi-active-io" not in (out or ""):
            raise OperationFailedError(
                f"Cross-client append/read failed on {reader_client.hostname}"
            )

        writer_client.exec_command(sudo=True, cmd=f"rm -rf {io_dir}")
        log.info("Basic IO validation completed on %s", io_dir)

    @staticmethod
    def unmount_and_delete_export(clients, nfs_mount, nfs_name, export_name):
        for client in clients:
            if not mount_cleanup_retry(client, nfs_mount):
                raise OperationFailedError(
                    f"Failed to cleanup mount directory on {client.hostname}"
                )
            err = NfsMultiActiveClient._umount_lazy_then_force(client, nfs_mount)
            if err:
                raise OperationFailedError(
                    f"Failed to unmount NFS on {client.hostname}: {err.strip()}"
                )
            client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}", check_ec=False)

        Ceph(clients[0]).nfs.export.delete(nfs_name, export_name)
        log.info("Removed export %s and unmounted clients", export_name)
