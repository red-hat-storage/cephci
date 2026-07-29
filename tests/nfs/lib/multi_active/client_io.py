"""Background IO (dd/fio) helpers for NFS Multi-Active failover tests."""

from datetime import datetime
from threading import Thread
from time import monotonic, sleep

from cli.exceptions import OperationFailedError
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.multi_active.constants import (
    BG_IO_RUNTIME,
    DD_BS,
    DD_COUNT,
    FIO_BS,
    FIO_IODEPTH,
    FIO_JOB_PREFIX,
    FIO_SIZE,
    log_section,
)
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveClientIO:
    """Background dd/fio IO, resume probes, and io_tool dispatch."""

    @staticmethod
    def _assert_io_dir_on_nfs(client, io_dir):
        """Fail fast if ``io_dir`` is not on an NFS mount (avoid local-disk IO)."""
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"findmnt -n -T {io_dir} -o FSTYPE,SOURCE",
            check_ec=False,
        )
        fs_line = (out or "").strip().splitlines()[0] if (out or "").strip() else ""
        if "nfs" not in fs_line.lower():
            raise OperationFailedError(
                f"IO dir {io_dir} is not on an NFS mount on {client.hostname}: "
                f"{fs_line or 'findmnt returned no output'!r}"
            )
        log.info("IO dir %s is on NFS (%s)", io_dir, fs_line)

    @staticmethod
    def _dd_data_file(io_dir, client):
        return f"{io_dir}/dd_{client.hostname}.dat"

    @staticmethod
    def _dd_stop_file(io_dir):
        return f"{io_dir}/.dd_stop"

    @staticmethod
    def _probe_nfs_path_responsive(client, path, probe_timeout=20):
        """Return True when ``stat`` on ``path`` completes successfully within the timeout."""
        cmd_timeout = probe_timeout + 15
        _, err, exit_code, _ = client.exec_command(
            sudo=True,
            cmd=f"timeout {probe_timeout} stat {path}",
            check_ec=False,
            verbose=True,
            timeout=cmd_timeout,
        )
        if exit_code != 0:
            log.info(
                "%s: NFS path probe failed for %s (exit=%s err=%s)",
                client.hostname,
                path,
                exit_code,
                (err or "").strip(),
            )
            return False
        return True

    @staticmethod
    def _sessions_active(sessions):
        return [
            (client, thread, io_dir)
            for client, thread, _, io_dir in sessions
            if thread.is_alive()
        ]

    @staticmethod
    def _probe_io_dir_stall(client, nfs_mount, io_dir, probe_timeout=20):
        """Return True while ``io_dir`` is still reachable over NFS."""
        return NfsMultiActiveClientIO._probe_nfs_path_responsive(
            client, io_dir, probe_timeout=probe_timeout
        )

    @staticmethod
    def _probe_client_dd_io_resume(client, nfs_mount, io_dir, probe_timeout=20):
        """Return True when dd is still running and the mount responds to stat.

        Uses pgrep first (no NFS) then a lightweight stat on the mount point.
        Avoids write/cat probes that contend with the background dd loop on the
        same mount and falsely fail while dd is still active.
        """
        dd_file = NfsMultiActiveClientIO._dd_data_file(io_dir, client)
        hostname = client.hostname
        cmd_timeout = probe_timeout + 10

        out, err, exit_code, _ = client.exec_command(
            sudo=True,
            cmd=f"pgrep -af 'dd if=/dev/zero of={dd_file}'",
            check_ec=False,
            verbose=True,
            timeout=cmd_timeout,
        )
        if exit_code != 0 or not (out or "").strip():
            log.info(
                "%s: IO resume probe failed: dd not running for %s",
                hostname,
                dd_file,
            )
            return False

        return NfsMultiActiveClientIO._probe_nfs_path_responsive(
            client, nfs_mount, probe_timeout=probe_timeout
        )

    @staticmethod
    def _recovery_seconds_since_trigger(triggered_at):
        if not triggered_at:
            return None
        try:
            started = datetime.strptime(triggered_at, "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return None
        return (datetime.now() - started).total_seconds()

    @staticmethod
    def _wait_for_background_io_resume(
        active,
        nfs_mount,
        probe_fn,
        io_label,
        timeout=300,
        interval=10,
        probe_timeout=20,
        timings=None,
        skip_log_message=None,
        skip_timings_value="skipped (IO already finished)",
        require_live_io=False,
        triggered_at=None,
    ):
        if not active:
            log.info(
                skip_log_message
                or "No background IO threads running; skip IO resume wait"
            )
            if timings is not None:
                timings["io_resume"] = skip_timings_value
            return

        log_section(
            log,
            f"WAIT {io_label} IO RESUME — {len(active)} client(s): "
            f"{', '.join(c.hostname for c, _, _ in active)}",
        )

        wait_start = monotonic()
        last_pending = []
        while True:
            if monotonic() - wait_start >= timeout:
                break

            pending = []
            for client, thread, io_dir in active:
                if require_live_io and not thread.is_alive():
                    pending.append(client.hostname)
                    continue
                remaining = timeout - (monotonic() - wait_start)
                if remaining <= 0:
                    break
                effective_probe_timeout = min(probe_timeout, max(1, int(remaining)))
                if not probe_fn(
                    client,
                    nfs_mount,
                    io_dir,
                    probe_timeout=effective_probe_timeout,
                ):
                    pending.append(client.hostname)

            if not pending:
                elapsed = monotonic() - wait_start
                recovery_since_trigger = (
                    NfsMultiActiveClientIO._recovery_seconds_since_trigger(triggered_at)
                )
                if recovery_since_trigger is not None:
                    log.info(
                        "%s IO resumed on all active client(s) after %.1fs since "
                        "failover trigger (%.1fs since polling started)",
                        io_label,
                        recovery_since_trigger,
                        elapsed,
                    )
                    if timings is not None:
                        timings["io_resume"] = f"{recovery_since_trigger:.1f}s"
                else:
                    log.info(
                        "%s IO resumed on all active client(s) after %.1fs",
                        io_label,
                        elapsed,
                    )
                    if timings is not None:
                        timings["io_resume"] = f"{elapsed:.1f}s"
                return

            last_pending = pending
            remaining = max(0.0, timeout - (monotonic() - wait_start))
            if remaining <= 0:
                log.error(
                    "NFS mount/%s not responsive on %s; giving up (0s remaining)",
                    io_label,
                    pending,
                )
                break
            log.info(
                "NFS mount/%s not yet responsive on %s; retrying (%.0fs remaining)",
                io_label,
                pending,
                remaining,
            )
            sleep(min(interval, remaining))

        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {io_label} IO to resume on "
            f"{last_pending} after failover"
            + (
                " (background IO stopped on one or more clients)"
                if require_live_io and last_pending
                else ""
            )
        )

    @staticmethod
    def _wait_for_background_io_stall(
        active,
        nfs_mount,
        probe_fn,
        io_label,
        timeout=300,
        interval=5,
        probe_timeout=20,
        timings=None,
    ):
        if not active:
            raise OperationFailedError(
                f"No background {io_label} IO sessions to confirm stall"
            )

        log_section(
            log,
            f"WAIT {io_label} IO STALL — {len(active)} client(s): "
            f"{', '.join(c.hostname for c, _, _ in active)}",
        )

        wait_start = monotonic()
        last_responsive = []
        while monotonic() - wait_start < timeout:
            responsive = []
            for client, thread, io_dir in active:
                if not thread.is_alive():
                    continue
                remaining = timeout - (monotonic() - wait_start)
                if remaining <= 0:
                    break
                effective_probe_timeout = min(probe_timeout, max(1, int(remaining)))
                if probe_fn(
                    client,
                    nfs_mount,
                    io_dir,
                    probe_timeout=effective_probe_timeout,
                ):
                    responsive.append(client.hostname)

            if not responsive:
                elapsed = monotonic() - wait_start
                log.info(
                    "%s IO stall confirmed on all active client(s) after %.1fs",
                    io_label,
                    elapsed,
                )
                if timings is not None:
                    timings["io_stall"] = f"{elapsed:.1f}s"
                return

            last_responsive = responsive
            remaining = max(0.0, timeout - (monotonic() - wait_start))
            if remaining <= 0:
                break
            log.info(
                "%s IO still responsive on %s; waiting for stall (%.0fs remaining)",
                io_label,
                responsive,
                remaining,
            )
            sleep(min(interval, remaining))

        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {io_label} IO to stall; "
            f"still responsive on {last_responsive}"
        )

    @staticmethod
    def _wait_for_io_stall(
        sessions,
        nfs_mount,
        probe_fn,
        io_label,
        timeout=300,
        interval=5,
        probe_timeout=20,
        timings=None,
    ):
        """Wait until background IO is no longer responsive on the NFS mount."""
        active = NfsMultiActiveClientIO._sessions_active(sessions)
        NfsMultiActiveClientIO._wait_for_background_io_stall(
            active,
            nfs_mount,
            probe_fn,
            io_label,
            timeout=timeout,
            interval=interval,
            probe_timeout=probe_timeout,
            timings=timings,
        )

    @staticmethod
    def _wait_for_io_resume(
        sessions,
        nfs_mount,
        probe_fn,
        io_label,
        skip_log_message,
        skip_timings_value,
        timeout=300,
        interval=10,
        probe_timeout=20,
        timings=None,
        triggered_at=None,
    ):
        """Wait until NFS IO is responsive and the background tool is still running."""
        active = NfsMultiActiveClientIO._sessions_active(sessions)
        NfsMultiActiveClientIO._wait_for_background_io_resume(
            active,
            nfs_mount,
            probe_fn,
            io_label,
            timeout=timeout,
            interval=interval,
            probe_timeout=probe_timeout,
            timings=timings,
            skip_log_message=skip_log_message,
            skip_timings_value=skip_timings_value,
            require_live_io=True,
            triggered_at=triggered_at,
        )

    @staticmethod
    def _run_dd(client, nfs_mount, io_subdir="failover_io", runtime=BG_IO_RUNTIME):
        """Start continuous dd writes in a background thread; return (thread, error_box, io_dir)."""
        io_dir = f"{nfs_mount.rstrip('/')}/{io_subdir}"
        error_box = {"exc": None, "stopped": False}
        dd_file = NfsMultiActiveClientIO._dd_data_file(io_dir, client)
        stop_file = NfsMultiActiveClientIO._dd_stop_file(io_dir)

        def _dd_worker():
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"mkdir -p {io_dir} && chown cephuser:cephuser {io_dir}",
                )
                NfsMultiActiveClientIO._assert_io_dir_on_nfs(client, io_dir)
                dd_cmd = (
                    f"timeout {runtime} sh -c "
                    f"'rm -f {stop_file}; "
                    f"while [ ! -f {stop_file} ]; do "
                    f"dd if=/dev/zero of={dd_file} bs={DD_BS} count={DD_COUNT} "
                    f"conv=fsync status=none; sleep 1; done'"
                )
                log_section(
                    log, f"START DD — continuous write on {client.hostname} at {io_dir}"
                )
                log.info("DD command on %s: %s", client.hostname, dd_cmd)
                client.exec_command(sudo=True, long_running=True, cmd=dd_cmd)
                if not error_box.get("stopped"):
                    log_section(log, f"END DD — completed on {client.hostname}")
            except Exception as exc:
                if not error_box.get("stopped"):
                    error_box["exc"] = exc
                    log_section(log, f"END DD — failed on {client.hostname}: {exc}")
                    log.error("Background dd failed on %s: %s", client.hostname, exc)

        thread = Thread(target=_dd_worker, daemon=True)
        thread.start()
        return thread, error_box, io_dir

    @staticmethod
    def _stop_dd(client, io_dir, error_box=None):
        """Stop the background dd loop on the client."""
        if error_box is not None:
            error_box["stopped"] = True
        stop_file = NfsMultiActiveClientIO._dd_stop_file(io_dir)
        dd_file = NfsMultiActiveClientIO._dd_data_file(io_dir, client)
        client.exec_command(
            sudo=True,
            cmd=f"timeout 10 pkill -9 -f 'dd if=/dev/zero of={dd_file}'",
            check_ec=False,
            timeout=15,
        )
        client.exec_command(
            sudo=True,
            cmd=f"timeout 10 touch {stop_file}",
            check_ec=False,
            timeout=15,
        )
        log.info("Stopped background dd on %s", client.hostname)

    @staticmethod
    def _assert_dd_completed(
        thread,
        error_box,
        stop_client=None,
        io_dir=None,
        nfs_mount=None,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        """Join dd thread; optionally wait for IO resume, then stop dd."""
        if (
            stop_client is not None
            and nfs_mount
            and io_dir
            and thread.is_alive()
            and wait_io_resume_timeout > 0
        ):
            NfsMultiActiveClientIO._wait_for_io_resume(
                [(stop_client, thread, error_box, io_dir)],
                nfs_mount,
                NfsMultiActiveClientIO._probe_client_dd_io_resume,
                "DD",
                "No background dd threads still running; skip IO resume wait",
                "skipped (dd already finished)",
                timeout=wait_io_resume_timeout,
                timings=timings,
            )
        if stop_client is not None and io_dir is not None:
            NfsMultiActiveClientIO._stop_dd(stop_client, io_dir, error_box)
        thread.join(timeout=join_timeout)
        if thread.is_alive():
            raise OperationFailedError(
                f"Background dd did not exit within {join_timeout}s after stop request"
            )
        if error_box.get("exc") and not error_box.get("stopped"):
            raise OperationFailedError(f"Background dd failed: {error_box['exc']}")
        if error_box.get("stopped"):
            log_section(
                log,
                f"END DD — stopped after failover validation on "
                f"{stop_client.hostname if stop_client else 'client'}",
            )

    @staticmethod
    def _assert_dd_completed_on_clients(
        sessions,
        nfs_mount,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        """Wait for IO resume, stop and join dd on every client."""
        if wait_io_resume_timeout > 0:
            NfsMultiActiveClientIO._wait_for_io_resume(
                sessions,
                nfs_mount,
                NfsMultiActiveClientIO._probe_client_dd_io_resume,
                "DD",
                "No background dd threads still running; skip IO resume wait",
                "skipped (dd already finished)",
                timeout=wait_io_resume_timeout,
                timings=timings,
            )
        for client, thread, error_box, io_dir in sessions:
            if not thread.is_alive():
                if error_box.get("exc") and not error_box.get("stopped"):
                    raise OperationFailedError(
                        f"Background dd failed on {client.hostname}: "
                        f"{error_box['exc']}"
                    )
                raise OperationFailedError(
                    f"Background dd on {client.hostname} stopped before IO resume "
                    f"validation completed"
                )
            NfsMultiActiveClientIO._assert_dd_completed(
                thread,
                error_box,
                stop_client=client,
                io_dir=io_dir,
                join_timeout=join_timeout,
                wait_io_resume_timeout=0,
            )

    @staticmethod
    def _run_dd_on_clients(
        clients, nfs_mount, io_subdir="failover_io", runtime=BG_IO_RUNTIME
    ):
        """Start dd on each client; return [(client, thread, error_box, io_dir), ...]."""
        sessions = []
        for client in clients:
            client_subdir = f"{io_subdir}/{client.hostname}"
            thread, error_box, io_dir = NfsMultiActiveClientIO._run_dd(
                client, nfs_mount, io_subdir=client_subdir, runtime=runtime
            )
            sessions.append((client, thread, error_box, io_dir))
        log.info(
            "Started background dd on %d client(s): %s",
            len(sessions),
            ", ".join(client.hostname for client, _, _, _ in sessions),
        )
        return sessions

    @staticmethod
    def _stop_dd_on_clients(sessions):
        """Stop dd on all clients in ``sessions``."""
        for client, _, error_box, io_dir in sessions:
            NfsMultiActiveClientIO._stop_dd(client, io_dir, error_box)

    @staticmethod
    def _fio_job_name(client):
        safe_host = client.hostname.replace(".", "_").replace("-", "_")
        return f"{FIO_JOB_PREFIX}_{safe_host}"

    @staticmethod
    def _fio_data_file(io_dir, client):
        return f"{io_dir}/fio_{client.hostname}.dat"

    @staticmethod
    def _fio_stop_file(io_dir):
        return f"{io_dir}/.fio_stop"

    @staticmethod
    def ensure_fio(client):
        """Install fio on the client node when missing."""
        log.info("Ensuring fio is installed on %s", client.hostname)
        client.exec_command(
            sudo=True,
            cmd="rpm -q fio || yum install -y fio",
            check_ec=False,
        )

    @staticmethod
    def _probe_client_fio_io_resume(client, nfs_mount, io_dir, probe_timeout=20):
        """Return True when fio is still running and the mount responds to stat."""
        job_name = NfsMultiActiveClientIO._fio_job_name(client)
        fio_file = NfsMultiActiveClientIO._fio_data_file(io_dir, client)
        hostname = client.hostname
        cmd_timeout = probe_timeout + 10

        out, err, exit_code, _ = client.exec_command(
            sudo=True,
            cmd=f"pgrep -af 'fio.*{job_name}'",
            check_ec=False,
            verbose=True,
            timeout=cmd_timeout,
        )
        if exit_code != 0 or not (out or "").strip():
            log.info(
                "%s: IO resume probe failed: fio job %s not running (%s)",
                hostname,
                job_name,
                fio_file,
            )
            return False

        return NfsMultiActiveClientIO._probe_nfs_path_responsive(
            client, io_dir, probe_timeout=probe_timeout
        )

    @staticmethod
    def _run_fio(
        client,
        nfs_mount,
        io_subdir="failover_io",
        runtime=BG_IO_RUNTIME,
        fio_bs=FIO_BS,
        fio_size=FIO_SIZE,
        fio_iodepth=FIO_IODEPTH,
    ):
        """Start continuous fio writes in a background thread."""
        io_dir = f"{nfs_mount.rstrip('/')}/{io_subdir}"
        error_box = {"exc": None, "stopped": False}
        job_name = NfsMultiActiveClientIO._fio_job_name(client)
        fio_file = NfsMultiActiveClientIO._fio_data_file(io_dir, client)
        stop_file = NfsMultiActiveClientIO._fio_stop_file(io_dir)

        def _fio_worker():
            try:
                NfsMultiActiveClientIO.ensure_fio(client)
                client.exec_command(
                    sudo=True,
                    cmd=f"mkdir -p {io_dir} && chown cephuser:cephuser {io_dir}",
                )
                NfsMultiActiveClientIO._assert_io_dir_on_nfs(client, io_dir)
                fio_cmd = (
                    f"timeout {runtime + 120} sh -c "
                    f"'rm -f {stop_file}; "
                    f"while [ ! -f {stop_file} ]; do "
                    f"fio --name={job_name} --rw=randwrite --bs={fio_bs} "
                    f"--size={fio_size} --numjobs=1 --iodepth={fio_iodepth} "
                    f"--runtime=60 --time_based --direct=0 "
                    f"--filename={fio_file} --output=/dev/null --group_reporting; "
                    f"sleep 1; done'"
                )
                log_section(
                    log,
                    f"START FIO — continuous write on {client.hostname} at {io_dir}",
                )
                log.info("FIO command on %s: %s", client.hostname, fio_cmd)
                client.exec_command(sudo=True, long_running=True, cmd=fio_cmd)
                if not error_box.get("stopped"):
                    log_section(log, f"END FIO — completed on {client.hostname}")
            except Exception as exc:
                if not error_box.get("stopped"):
                    error_box["exc"] = exc
                    log_section(log, f"END FIO — failed on {client.hostname}: {exc}")
                    log.error("Background fio failed on %s: %s", client.hostname, exc)

        thread = Thread(target=_fio_worker, daemon=True)
        thread.start()
        return thread, error_box, io_dir

    @staticmethod
    def _stop_fio(client, io_dir, error_box=None):
        """Stop the background fio loop on the client."""
        if error_box is not None:
            error_box["stopped"] = True
        stop_file = NfsMultiActiveClientIO._fio_stop_file(io_dir)
        job_name = NfsMultiActiveClientIO._fio_job_name(client)
        client.exec_command(
            sudo=True,
            cmd=f"timeout 10 pkill -9 -f 'fio.*{job_name}'",
            check_ec=False,
            timeout=15,
        )
        client.exec_command(
            sudo=True,
            cmd=f"timeout 10 touch {stop_file}",
            check_ec=False,
            timeout=15,
        )
        log.info("Stopped background fio on %s", client.hostname)

    @staticmethod
    def _assert_fio_completed(
        thread,
        error_box,
        stop_client=None,
        io_dir=None,
        nfs_mount=None,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        """Join fio thread; optionally wait for IO resume, then stop fio."""
        if (
            stop_client is not None
            and nfs_mount
            and io_dir
            and thread.is_alive()
            and wait_io_resume_timeout > 0
        ):
            NfsMultiActiveClientIO._wait_for_io_resume(
                [(stop_client, thread, error_box, io_dir)],
                nfs_mount,
                NfsMultiActiveClientIO._probe_client_fio_io_resume,
                "FIO",
                "No background fio threads still running; skip IO resume wait",
                "skipped (fio already finished)",
                timeout=wait_io_resume_timeout,
                timings=timings,
            )
        if stop_client is not None and io_dir is not None:
            NfsMultiActiveClientIO._stop_fio(stop_client, io_dir, error_box)
        thread.join(timeout=join_timeout)
        if thread.is_alive():
            raise OperationFailedError(
                f"Background fio did not exit within {join_timeout}s after stop request"
            )
        if error_box.get("exc") and not error_box.get("stopped"):
            raise OperationFailedError(f"Background fio failed: {error_box['exc']}")
        if error_box.get("stopped"):
            log_section(
                log,
                f"END FIO — stopped after failover validation on "
                f"{stop_client.hostname if stop_client else 'client'}",
            )

    @staticmethod
    def _assert_fio_completed_on_clients(
        sessions,
        nfs_mount,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        """Wait for IO resume, stop and join fio on every client."""
        if wait_io_resume_timeout > 0:
            NfsMultiActiveClientIO._wait_for_io_resume(
                sessions,
                nfs_mount,
                NfsMultiActiveClientIO._probe_client_fio_io_resume,
                "FIO",
                "No background fio threads still running; skip IO resume wait",
                "skipped (fio already finished)",
                timeout=wait_io_resume_timeout,
                timings=timings,
            )
        for client, thread, error_box, io_dir in sessions:
            if not thread.is_alive():
                if error_box.get("exc") and not error_box.get("stopped"):
                    raise OperationFailedError(
                        f"Background fio failed on {client.hostname}: "
                        f"{error_box['exc']}"
                    )
                raise OperationFailedError(
                    f"Background fio on {client.hostname} stopped before IO resume "
                    f"validation completed"
                )
            NfsMultiActiveClientIO._assert_fio_completed(
                thread,
                error_box,
                stop_client=client,
                io_dir=io_dir,
                join_timeout=join_timeout,
                wait_io_resume_timeout=0,
            )

    @staticmethod
    def _run_fio_on_clients(
        clients,
        nfs_mount,
        io_subdir="failover_io",
        runtime=BG_IO_RUNTIME,
        fio_bs=FIO_BS,
        fio_size=FIO_SIZE,
        fio_iodepth=FIO_IODEPTH,
    ):
        """Start fio on each client; return [(client, thread, error_box, io_dir), ...]."""
        sessions = []
        for client in clients:
            client_subdir = f"{io_subdir}/{client.hostname}"
            thread, error_box, io_dir = NfsMultiActiveClientIO._run_fio(
                client,
                nfs_mount,
                io_subdir=client_subdir,
                runtime=runtime,
                fio_bs=fio_bs,
                fio_size=fio_size,
                fio_iodepth=fio_iodepth,
            )
            sessions.append((client, thread, error_box, io_dir))
        log.info(
            "Started background fio on %d client(s): %s",
            len(sessions),
            ", ".join(client.hostname for client, _, _, _ in sessions),
        )
        return sessions

    @staticmethod
    def _stop_fio_on_clients(sessions):
        """Stop fio on all clients in ``sessions``."""
        for client, _, error_box, io_dir in sessions:
            NfsMultiActiveClientIO._stop_fio(client, io_dir, error_box)

    @staticmethod
    def run_background_io(
        client, nfs_mount, config, io_subdir="failover_io", runtime=None
    ):
        """Start background IO using ``io_tool`` from suite config (``dd`` or ``fio``)."""
        runtime = runtime or NfsMultiActiveConfig.io_runtime(config)
        if NfsMultiActiveConfig.resolve_io_tool(config) == "fio":
            params = NfsMultiActiveConfig.fio_params(config)
            return NfsMultiActiveClientIO._run_fio(
                client,
                nfs_mount,
                io_subdir=io_subdir,
                runtime=runtime,
                fio_bs=params["bs"],
                fio_size=params["size"],
                fio_iodepth=params["iodepth"],
            )
        return NfsMultiActiveClientIO._run_dd(
            client, nfs_mount, io_subdir=io_subdir, runtime=runtime
        )

    @staticmethod
    def run_background_io_on_clients(
        clients, nfs_mount, config, io_subdir="failover_io", runtime=None
    ):
        """Start background IO on each client per ``io_tool`` in config."""
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        runtime = runtime or NfsMultiActiveConfig.io_runtime(config)
        if tool == "fio":
            params = NfsMultiActiveConfig.fio_params(config)
            return NfsMultiActiveClientIO._run_fio_on_clients(
                clients,
                nfs_mount,
                io_subdir=io_subdir,
                runtime=runtime,
                fio_bs=params["bs"],
                fio_size=params["size"],
                fio_iodepth=params["iodepth"],
            )
        return NfsMultiActiveClientIO._run_dd_on_clients(
            clients, nfs_mount, io_subdir=io_subdir, runtime=runtime
        )

    @staticmethod
    def stop_background_io(client, io_dir, config, error_box=None):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            NfsMultiActiveClientIO._stop_fio(client, io_dir, error_box)
        else:
            NfsMultiActiveClientIO._stop_dd(client, io_dir, error_box)

    @staticmethod
    def stop_background_io_on_clients(sessions, config):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            NfsMultiActiveClientIO._stop_fio_on_clients(sessions)
        else:
            NfsMultiActiveClientIO._stop_dd_on_clients(sessions)

    @staticmethod
    def wait_for_background_io_stall(
        sessions,
        nfs_mount,
        config,
        timeout=300,
        interval=5,
        probe_timeout=20,
        timings=None,
    ):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        io_label = "FIO" if tool == "fio" else "DD"
        NfsMultiActiveClientIO._wait_for_io_stall(
            sessions,
            nfs_mount,
            NfsMultiActiveClientIO._probe_io_dir_stall,
            io_label,
            timeout=timeout,
            interval=interval,
            probe_timeout=probe_timeout,
            timings=timings,
        )

    @staticmethod
    def wait_for_background_io_resume(
        sessions,
        nfs_mount,
        config,
        timeout=300,
        interval=10,
        probe_timeout=20,
        timings=None,
        triggered_at=None,
    ):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            NfsMultiActiveClientIO._wait_for_io_resume(
                sessions,
                nfs_mount,
                NfsMultiActiveClientIO._probe_client_fio_io_resume,
                "FIO",
                "No background fio threads still running; skip IO resume wait",
                "skipped (fio already finished)",
                timeout=timeout,
                interval=interval,
                probe_timeout=probe_timeout,
                timings=timings,
                triggered_at=triggered_at,
            )
            return
        NfsMultiActiveClientIO._wait_for_io_resume(
            sessions,
            nfs_mount,
            NfsMultiActiveClientIO._probe_client_dd_io_resume,
            "DD",
            "No background dd threads still running; skip IO resume wait",
            "skipped (dd already finished)",
            timeout=timeout,
            interval=interval,
            probe_timeout=probe_timeout,
            timings=timings,
            triggered_at=triggered_at,
        )

    @staticmethod
    def assert_io_completed(
        thread,
        error_box,
        config,
        stop_client=None,
        io_dir=None,
        nfs_mount=None,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            return NfsMultiActiveClientIO._assert_fio_completed(
                thread,
                error_box,
                stop_client=stop_client,
                io_dir=io_dir,
                nfs_mount=nfs_mount,
                join_timeout=join_timeout,
                wait_io_resume_timeout=wait_io_resume_timeout,
                timings=timings,
            )
        return NfsMultiActiveClientIO._assert_dd_completed(
            thread,
            error_box,
            stop_client=stop_client,
            io_dir=io_dir,
            nfs_mount=nfs_mount,
            join_timeout=join_timeout,
            wait_io_resume_timeout=wait_io_resume_timeout,
            timings=timings,
        )

    @staticmethod
    def assert_io_completed_on_clients(
        sessions,
        nfs_mount,
        config,
        join_timeout=120,
        wait_io_resume_timeout=300,
        timings=None,
    ):
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            return NfsMultiActiveClientIO._assert_fio_completed_on_clients(
                sessions,
                nfs_mount,
                join_timeout=join_timeout,
                wait_io_resume_timeout=wait_io_resume_timeout,
                timings=timings,
            )
        return NfsMultiActiveClientIO._assert_dd_completed_on_clients(
            sessions,
            nfs_mount,
            join_timeout=join_timeout,
            wait_io_resume_timeout=wait_io_resume_timeout,
            timings=timings,
        )
