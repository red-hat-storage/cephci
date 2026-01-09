import json
import re
import time
from typing import Any, Dict, List, Optional, Set

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class MDSMetricsHelper:
    """
    Query CephFS MDS 'counter dump' and extract subvolume metrics, selecting
    MDS targets by role (active / standby-replay) and rank.

    Also provides helpers to:
      - poll snapshots during a run
      - launch/wait for fio (background)
      - parse fio summary output
      - compare fio stats vs MDS counters (read/write/both)
    """

    def __init__(self, ceph_cluster, **kwargs):
        self.fs_util = FsUtils(ceph_cluster, test_data=kwargs.get("test_data", {}))
        self.clients = self.fs_util.clients

        # Ceph 'tell' shell timeouts / retries (to avoid long stalls)
        self.tell_timeout_s = int(kwargs.get("tell_timeout_s", 12))
        self.tell_retries = int(kwargs.get("tell_retries", 2))
        self.tell_retry_sleep = int(kwargs.get("tell_retry_sleep", 2))

    # ---------------------------------------------------------------------
    # Rank-aware selection
    # ---------------------------------------------------------------------

    def get_pairs_by_ranks(
        self,
        client=None,
        fs_name: str = "cephfs",
        ranks: Optional[List[int]] = None,
    ) -> Dict[int, Dict[str, Any]]:
        client = client or self.clients[0]
        pairs = self.fs_util.get_mds_states_active_standby_replay(fs_name, client)
        if not isinstance(pairs, dict):
            log.error(
                "Unexpected result from get_mds_states_active_standby_replay: %r", pairs
            )
            return {}
        if ranks is None:
            return pairs
        rank_set: Set[int] = set(ranks)
        return {r: pairs[r] for r in pairs.keys() if r in rank_set}

    def get_active_by_ranks(
        self,
        client=None,
        fs_name: str = "cephfs",
        ranks: Optional[List[int]] = None,
    ) -> List[str]:
        pairs = self.get_pairs_by_ranks(client=client, fs_name=fs_name, ranks=ranks)
        targets: List[str] = []
        for _, info in sorted(pairs.items()):
            name = info.get("active")
            if name:
                targets.append(name)
        return targets

    def get_standby_replay_by_ranks(
        self,
        client=None,
        fs_name: str = "cephfs",
        ranks: Optional[List[int]] = None,
        pick: str = "all",
    ) -> List[str]:
        pairs = self.get_pairs_by_ranks(client=client, fs_name=fs_name, ranks=ranks)
        targets: List[str] = []
        for _, info in sorted(pairs.items()):
            sbr = info.get("standby-replay") or []
            if not isinstance(sbr, list):
                continue
            if pick == "first":
                if sbr:
                    targets.append(sbr[0])
            else:
                targets.extend(sbr)
        # dedupe preserve order
        seen = set()
        uniq = []
        for t in targets:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    # ---------------------------------------------------------------------
    # Metrics collection
    # ---------------------------------------------------------------------

    def collect_subvolume_metrics(
        self,
        client=None,
        fs_name: str = "cephfs",
        role: str = "active",  # "active" | "standby-replay" | "both"
        ranks: Optional[List[int]] = None,
        path_prefix: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect 'mds_subvolume_metrics' from selected MDS daemons.

        Returns:
          { "<mds_name>": [ {fs_name, subvolume_path, avg_*...}, ... ], ... }
        """
        client = client or self.clients[0]
        role = role.lower()
        if role not in ("active", "standby-replay", "both"):
            raise ValueError("role must be one of: 'active', 'standby-replay', 'both'")

        # Normalize the prefix so it matches the label format in counters
        norm_prefix = (
            self._normalize_metrics_prefix(path_prefix) if path_prefix else None
        )
        if path_prefix and norm_prefix != path_prefix:
            log.info(
                "Normalizing subvolume filter from %r to %r for metrics matching",
                path_prefix,
                norm_prefix,
            )

        targets: List[str] = []
        if role in ("active", "both"):
            targets.extend(self.get_active_by_ranks(client, fs_name, ranks))
        if role in ("standby-replay", "both"):
            targets.extend(
                self.get_standby_replay_by_ranks(client, fs_name, ranks, pick="all")
            )

        # Deduplicate while preserving order
        seen = set()
        uniq_targets: List[str] = []
        for t in targets:
            if t not in seen:
                seen.add(t)
                uniq_targets.append(t)

        results: Dict[str, List[Dict[str, Any]]] = {}
        for mds_name in uniq_targets:
            try:
                # Prefer jq (robust to banner noise), with a hard timeout + retries
                items = self._mds_counter_dump_metrics_jq(
                    client, mds_name=mds_name, fs_name=fs_name, path_prefix=norm_prefix
                )
                if items is None:
                    dump = self._mds_counter_dump_json(client, mds_name)
                    items = dump.get("mds_subvolume_metrics", []) or []

                flat = self._flatten_subvolume_items(
                    items, fs_filter=fs_name, path_prefix=norm_prefix
                )
                if flat:
                    results[mds_name] = flat
            except Exception as e:
                log.error("Failed to collect metrics from %s: %s", mds_name, e)
        return results

    def collect_mds_metrics(
        self,
        client=None,
        fs_name: str = "cephfs",
        role: str = "active",  # "active" | "standby-replay" | "both"
        ranks: Optional[List[int]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect 'mds_metrics' from selected MDS daemons.

        Returns:
          { "<mds_name>": [ {fs_name, rank, cpu_usage, open_requests...}, ... ], ... }
        """
        client = client or self.clients[0]
        role = role.lower()
        if role not in ("active", "standby-replay", "both"):
            raise ValueError("role must be one of: 'active', 'standby-replay', 'both'")

        # Normalize the prefix so it matches the label format in counters

        targets: List[str] = []
        if role in ("active"):
            targets.extend(self.get_active_by_ranks(client, fs_name, ranks))
        if role in ("standby-replay", "standby"):
            targets.extend(
                self.get_standby_replay_by_ranks(client, fs_name, ranks, pick="all")
            )

        # Deduplicate while preserving order
        seen = set()
        uniq_targets: List[str] = []
        for t in targets:
            if t not in seen:
                seen.add(t)
                uniq_targets.append(t)

        results: Dict[str, List[Dict[str, Any]]] = {}
        for mds_name in uniq_targets:
            try:
                # For mds_rank_perf, use JSON dump directly (jq method is for subvolume metrics)
                dump = self._mds_counter_dump_json(client, mds_name)
                items = dump.get("mds_rank_perf", []) or []
                if items:
                    results[mds_name] = items
            except Exception as e:
                log.error("Failed to collect metrics from %s: %s", mds_name, e)
        return results

    # ---------------------------------------------------------------------
    # Snapshot polling during a run
    # ---------------------------------------------------------------------

    def poll_metrics_during_run(
        self,
        client,
        fs_name: str,
        subvol_path: str,
        ranks: Optional[List[int]],
        role: str,
        duration_sec: int = 300,
        interval_sec: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Sample MDS subvolume metrics every interval during a run.
        Returns:
          [{ 't': <epoch>, 'samples': { 'mds.name': [metric_items...] , ... } }, ...]
        """
        snapshots: List[Dict[str, Any]] = []
        start = time.time()
        next_t = start
        end = start + duration_sec

        while True:
            now = time.time()
            if now >= next_t:
                sample = self.collect_subvolume_metrics(
                    client=client,
                    fs_name=fs_name,
                    role=role,
                    ranks=ranks,
                    path_prefix=subvol_path,
                )
                log.info(sample)
                snapshots.append({"t": int(now), "samples": sample})
                next_t += interval_sec
            if now >= end:
                break
            time.sleep(1)
        return snapshots

    # ---------------------------------------------------------------------
    # Internals
    # ---------------------------------------------------------------------

    _UUID_RE = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )

    def _normalize_metrics_prefix(self, p: Optional[str]) -> Optional[str]:
        """
        Ceph 'mds_subvolume_metrics.labels.subvolume_path' is typically:
          /volumes/_nogroup/<subvol_name>
        But 'ceph fs subvolume getpath' may return:
          /volumes/_nogroup/<subvol_name>/<uuid>

        This normalizes to the base path so filtering matches counters.
        """
        if not p or not p.startswith("/"):
            return p
        parts = [x for x in p.strip("/").split("/") if x]
        # If looks like /volumes/_nogroup/<subvol>[/<uuid>...], keep first 3 segments
        if len(parts) >= 3 and parts[0] == "volumes":
            base = "/" + "/".join(parts[:3])
            return base
        # Otherwise, if last segment is a UUID, drop it
        if parts and self._UUID_RE.match(parts[-1]):
            return "/" + "/".join(parts[:-1])
        return p

    def _mds_counter_dump_json(self, client, mds_name: str) -> Dict[str, Any]:
        """
        Run 'ceph tell mds.<name> counter dump -f json' and parse strictly.
        """
        cmd = f"ceph tell mds.{mds_name} counter dump -f json"
        out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        return json.loads(out)

    def _mds_counter_dump_metrics_jq(
        self,
        client,
        mds_name: str,
        fs_name: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Use jq to emit ONLY the 'mds_subvolume_metrics' array, filtered by fs_name
        and subvolume prefix. Protected by a shell timeout and quick retries.

        Returns a Python list of items, or None if jq is unavailable / fails.
        """
        jq_filter = ".mds_subvolume_metrics // []"
        if fs_name:
            jq_filter += f' | map(select(.labels.fs_name == "{fs_name}"))'
        if path_prefix:
            jq_filter += (
                f' | map(select(.labels.subvolume_path | startswith("{path_prefix}")))'
            )

        pipeline = (
            f"ceph tell mds.{mds_name} counter dump -f json 2>/dev/null | "
            r"sed -n '/^{/,$p' | "
            f"jq -c '{jq_filter}'"
        )
        # Wrap with GNU timeout and bash -lc to ensure the whole pipeline is bounded
        timed_cmd = f"timeout {self.tell_timeout_s}s bash -lc {json.dumps(pipeline)}"

        last_err: Optional[Exception] = None
        for attempt in range(self.tell_retries + 1):
            try:
                out, _ = client.exec_command(sudo=True, cmd=timed_cmd, check_ec=False)
                s = (out or "").strip()
                if not s:
                    raise RuntimeError("empty output from ceph tell/jq")
                return json.loads(s)
            except Exception as e:
                last_err = e
                if attempt < self.tell_retries:
                    time.sleep(self.tell_retry_sleep)
                else:
                    log.error(
                        "tell/jq failed for %s after %d attempts: %s",
                        mds_name,
                        self.tell_retries + 1,
                        last_err,
                    )
                    return None

    def _flatten_subvolume_items(
        self,
        items: List[Dict[str, Any]],
        fs_filter: Optional[str],
        path_prefix: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        Flatten each item (labels + counters) and apply optional filters.
        """
        out: List[Dict[str, Any]] = []
        if not isinstance(items, list):
            return out

        for item in items:
            labels = item.get("labels", {}) or {}
            counters = item.get("counters", {}) or {}
            fs_name = labels.get("fs_name")
            subvol_path = labels.get("subvolume_path")

            if fs_filter and fs_name != fs_filter:
                continue
            if path_prefix and (
                not isinstance(subvol_path, str)
                or not subvol_path.startswith(path_prefix)
            ):
                continue

            out.append(
                {
                    "fs_name": fs_name,
                    "subvolume_path": subvol_path,
                    "avg_read_iops": counters.get("avg_read_iops", 0),
                    "avg_read_tp_Bps": counters.get("avg_read_tp_Bps", 0),
                    "avg_read_lat_msec": counters.get("avg_read_lat_msec", 0),
                    "avg_write_iops": counters.get("avg_write_iops", 0),
                    "avg_write_tp_Bps": counters.get("avg_write_tp_Bps", 0),
                    "avg_write_lat_msec": counters.get("avg_write_lat_msec", 0),
                    "last_window_end_sec": counters.get("last_window_end_sec", 0),
                    "last_window_dur_sec": counters.get("last_window_dur_sec", 0),
                    "quota_bytes": counters.get("quota_bytes", 0),
                    "used_bytes": counters.get("used_bytes", 0),
                }
            )
        return out

    # ---------------------------------------------------------------------
    # FIO helpers (launch/wait/parse/compare)
    # ---------------------------------------------------------------------

    @staticmethod
    def run_fio_background(
        client,
        target_dir: str,
        runtime_sec: int = 300,
        jobname: str = "fio_subvol_test",
        rw: str = "randwrite",  # "randread", "randwrite", "randrw", "read", "write", "readwrite"
        bs: str = "4k",
        iodepth: int = 1,
        numjobs: int = 4,
        size: str = "1G",
        rwmixread: Optional[int] = None,  # e.g. 70 (only used for *rw modes)
    ) -> tuple[str, str]:
        """
        Launch fio on the client in the background and return (PID, LOG_PATH).
        """
        log_path = f"/root/{jobname}.log"
        mix = (
            f" --rwmixread={rwmixread}" if rwmixread is not None and "rw" in rw else ""
        )
        cmd = (
            f"nohup fio --name={jobname}"
            f" --directory={target_dir}"
            f" --rw={rw}{mix}"
            f" --bs={bs} --iodepth={iodepth} --numjobs={numjobs}"
            f" --size={size}"
            f" --time_based=1 --runtime={runtime_sec} --group_reporting=1"
            f" --ioengine=libaio --direct=1 --randrepeat=0 --norandommap"
            f" > {log_path} 2>&1 & echo $!"
        )
        out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        pid = out.strip()
        log.info(
            f"Started fio (pid={pid}) rw={rw}{mix} in {target_dir}, log: {log_path}"
        )
        return pid, log_path

    @staticmethod
    def wait_pid(client, pid: str, timeout: int) -> None:
        client.exec_command(
            sudo=True,
            cmd=f"timeout {timeout}s bash -lc 'while kill -0 {pid} 2>/dev/null; do sleep 2; done'",
        )

    @staticmethod
    def parse_fio_summary(text: str) -> Dict[str, Any]:
        """
        Parse fio summary (group_reporting) and return:
          {
            "read_iops": float|None,  "read_bw_Bps": float|None,  "read_lat_ms": float|None,
            "write_iops": float|None, "write_bw_Bps": float|None, "write_lat_ms": float|None,
          }
        Supports IOPS suffixes (k/M) and BW units (KiB/MiB/GiB).
        """

        def _scale_iops(v: str) -> float:
            m = re.match(r"^(\d+(?:\.\d+)?)([kKmM]?)$", v)
            if not m:
                return float(v)
            val = float(m.group(1))
            sfx = m.group(2).lower()
            if sfx == "k":
                val *= 1_000.0
            elif sfx == "m":
                val *= 1_000_000.0
            return val

        def _bw_to_Bps(val: str, unit: str) -> float:
            valf = float(val)
            unit = unit.lower()
            mult = 1.0
            if unit == "kib":
                mult = 1024.0
            elif unit == "mib":
                mult = 1024.0**2
            elif unit == "gib":
                mult = 1024.0**3
            return valf * mult

        out = {
            "read_iops": None,
            "read_bw_Bps": None,
            "read_lat_ms": None,
            "write_iops": None,
            "write_bw_Bps": None,
            "write_lat_ms": None,
        }

        # IOPS/BW lines (e.g. "read: IOPS=123, BW=456KiB/s", "write: IOPS=1.2k, BW=3.4MiB/s")
        for which in ("read", "write"):
            m = re.search(
                rf"{which}:\s+IOPS=(\d+(?:\.\d+)?[kKmM]?)\s*,\s*BW=(\d+(?:\.\d+)?)([KMG]iB)/s",
                text,
            )
            if m:
                out[f"{which}_iops"] = _scale_iops(m.group(1))
                out[f"{which}_bw_Bps"] = _bw_to_Bps(m.group(2), m.group(3))

        # Latency: walk line-by-line and attribute the next 'lat (...) : ... avg=' to the last seen op section
        current = None  # "read" or "write"
        for line in text.splitlines():
            if line.lstrip().startswith("read:"):
                current = "read"
            elif line.lstrip().startswith("write:"):
                current = "write"
            m = re.search(r"lat\s+\((usec|msec)\)\s*:\s.*avg=\s*([\d\.]+)", line)
            if m and current in ("read", "write"):
                unit, avg = m.group(1).lower(), float(m.group(2))
                out[f"{current}_lat_ms"] = avg / 1000.0 if unit == "usec" else avg

        # Fallbacks for single-op jobs
        if out["write_lat_ms"] is None and "write:" in text and "read:" not in text:
            m1 = re.search(r"lat\s+\((usec|msec)\)\s*:\s.*avg=\s*([\d\.]+)", text)
            if m1:
                out["write_lat_ms"] = float(m1.group(2)) / (
                    1000.0 if m1.group(1).lower() == "usec" else 1.0
                )
        if out["read_lat_ms"] is None and "read:" in text and "write:" not in text:
            m2 = re.search(r"lat\s+\((usec|msec)\)\s*:\s.*avg=\s*([\d\.]+)", text)
            if m2:
                out["read_lat_ms"] = float(m2.group(2)) / (
                    1000.0 if m2.group(1).lower() == "usec" else 1.0
                )

        return out
