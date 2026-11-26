"""
Collate cephadm agent scale-regression timings into JSON / HTML.

During a CephCI run each TC writes::
  <log_dir>/agent_scale_timings_<case>.json
  <log_dir>/agent_scale_timings.json          (aggregate)
  <log_dir>/agent_scale_timings.html         (single-run report)

Offline compare of two log dirs (or two aggregate JSON files)::

  python3 -m utility.collate_scale_timings \\
      /path/to/runA /path/to/runB -o /tmp/compare.html

Also parses ``TIMING [...]`` lines from ``run.log`` when JSON is missing
(older runs / copied logs).
"""

from __future__ import annotations

import argparse
import ast
import html
import json
import os
import re
import shutil
import sys
from collections import OrderedDict
from typing import Any, Dict, List, Optional

TIMINGS_AGGREGATE_NAME = "agent_scale_timings.json"
TIMINGS_HTML_NAME = "agent_scale_timings.html"
TIMING_LINE_RE = re.compile(
    r"TIMING \[([^\]]+)\]\s+([0-9.]+)s\s+\(([^)]*)\s*->\s*([^)]*)\)\s+meta=(.*)\s*$"
)


def write_case_timings(log_dir: str, case_payload: Dict[str, Any]) -> str:
    """Write per-case JSON and merge into the aggregate file. Returns case path."""
    os.makedirs(log_dir, exist_ok=True)
    case_id = str(case_payload.get("case") or "unknown")
    case_path = os.path.join(log_dir, "agent_scale_timings_{0}.json".format(case_id))
    with open(case_path, "w", encoding="utf-8") as fh:
        json.dump(case_payload, fh, indent=2, default=str, sort_keys=False)
        fh.write("\n")

    aggregate = _load_aggregate(log_dir)
    cases = aggregate.setdefault("cases", OrderedDict())
    # Prefer OrderedDict-friendly list storage
    if isinstance(cases, list):
        cases = OrderedDict((c.get("case"), c) for c in cases if c.get("case"))
    cases[case_id] = case_payload
    aggregate["cases"] = list(cases.values())
    aggregate["updated_at"] = case_payload.get("finished_at") or case_payload.get(
        "started_at"
    )
    aggregate["log_dir"] = log_dir
    agg_path = os.path.join(log_dir, TIMINGS_AGGREGATE_NAME)
    with open(agg_path, "w", encoding="utf-8") as fh:
        json.dump(aggregate, fh, indent=2, default=str)
        fh.write("\n")
    return case_path


def _load_aggregate(log_dir: str) -> Dict[str, Any]:
    path = os.path.join(log_dir, TIMINGS_AGGREGATE_NAME)
    if not os.path.isfile(path):
        return {"cases": []}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {"cases": []}
        data.setdefault("cases", [])
        return data
    except (OSError, ValueError):
        return {"cases": []}


def parse_timing_lines_from_log(text: str) -> List[Dict[str, Any]]:
    """Extract TIMING records from a CephCI log blob."""
    out: List[Dict[str, Any]] = []
    for line in text.splitlines():
        if "TIMING [" not in line:
            continue
        # strip logger prefix up to TIMING
        idx = line.find("TIMING [")
        payload = line[idx:]
        m = TIMING_LINE_RE.search(payload)
        if not m:
            continue
        name, seconds, started, finished, meta_s = m.groups()
        meta: Any
        try:
            meta = ast.literal_eval(meta_s)
        except Exception:
            meta = {"raw": meta_s}
        out.append(
            {
                "name": name,
                "seconds": float(seconds),
                "started_at": started.strip(),
                "finished_at": finished.strip(),
                "meta": meta if isinstance(meta, dict) else {"value": meta},
            }
        )
    return out


def load_run_timings(path: str) -> Dict[str, Any]:
    """
    Load timings from a log directory or an aggregate JSON file.

    Preference order:
      1) agent_scale_timings.json
      2) agent_scale_timings_*.json fragments
      3) TIMING lines from run.log / *TC*.log
    """
    if os.path.isfile(path) and path.endswith(".json"):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if "cases" not in data and "timings" in data:
            return {"cases": [data], "source": path}
        data.setdefault("cases", [])
        data["source"] = path
        return data

    log_dir = path
    agg_path = os.path.join(log_dir, TIMINGS_AGGREGATE_NAME)
    if os.path.isfile(agg_path):
        data = _load_aggregate(log_dir)
        data["source"] = agg_path
        if data.get("cases"):
            return data

    cases: List[Dict[str, Any]] = []
    if os.path.isdir(log_dir):
        for name in sorted(os.listdir(log_dir)):
            if not (
                name.startswith("agent_scale_timings_")
                and name.endswith(".json")
                and name != TIMINGS_AGGREGATE_NAME
            ):
                continue
            with open(os.path.join(log_dir, name), encoding="utf-8") as fh:
                cases.append(json.load(fh))
    if cases:
        return {"cases": cases, "source": log_dir, "log_dir": log_dir}

    # Fallback: scrape logs
    timings = []
    if os.path.isdir(log_dir):
        candidates = []
        run_log = os.path.join(log_dir, "run.log")
        if os.path.isfile(run_log):
            candidates.append(run_log)
        for name in sorted(os.listdir(log_dir)):
            if "TC" in name and name.endswith(".log"):
                candidates.append(os.path.join(log_dir, name))
        seen = set()
        for cpath in candidates:
            with open(cpath, encoding="utf-8", errors="ignore") as fh:
                for rec in parse_timing_lines_from_log(fh.read()):
                    key = (
                        rec["name"],
                        rec["started_at"],
                        rec["finished_at"],
                        rec["seconds"],
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    timings.append(rec)
    if timings:
        return {
            "cases": [
                {
                    "case": "from_logs",
                    "timings": timings,
                }
            ],
            "source": log_dir,
            "log_dir": log_dir,
            "parsed_from_logs": True,
        }
    return {"cases": [], "source": path}


def flatten_metrics(run: Dict[str, Any]) -> "OrderedDict[str, float]":
    """Map metric key -> seconds. Key = case/name (or name when single scraped case)."""
    metrics: "OrderedDict[str, float]" = OrderedDict()
    for case in run.get("cases") or []:
        case_id = str(case.get("case") or "case")
        for t in case.get("timings") or []:
            name = str(t.get("name") or "unnamed")
            key = "{0}/{1}".format(case_id, name)
            try:
                metrics[key] = float(t.get("seconds"))
            except (TypeError, ValueError):
                continue
    return metrics


def compare_runs(run_a: Dict[str, Any], run_b: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return rows with metric, a, b, delta, pct."""
    a = flatten_metrics(run_a)
    b = flatten_metrics(run_b)
    keys = list(OrderedDict.fromkeys(list(a.keys()) + list(b.keys())))
    rows = []
    for key in keys:
        va = a.get(key)
        vb = b.get(key)
        delta = None
        pct = None
        if va is not None and vb is not None:
            delta = round(vb - va, 2)
            if va != 0:
                pct = round(100.0 * (vb - va) / va, 1)
        rows.append(
            {
                "metric": key,
                "a": va,
                "b": vb,
                "delta": delta,
                "pct": pct,
            }
        )
    return rows


def _fmt_sec(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return "{0:.2f}".format(value)


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def render_html_single(run: Dict[str, Any], title: Optional[str] = None) -> str:
    title = title or "Cephadm agent scale timings"
    parts = [
        "<!DOCTYPE html>",
        '<html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>{0}</title>".format(_esc(title)),
        "<style>",
        "body{font:15px/1.45 system-ui,sans-serif;margin:0;background:#f5f6f8;color:#1a1d23}",
        "header{background:#1a2332;color:#fff;padding:20px 24px}",
        "header h1{margin:0 0 4px;font-size:1.25rem}",
        "header p{margin:0;opacity:.85;font-size:.9rem}",
        "main{max-width:1100px;margin:0 auto;padding:18px 16px 40px}",
        "h2{font-size:1.05rem;margin:22px 0 8px;border-bottom:2px solid #e2e5ea;padding-bottom:4px}",
        "table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e2e5ea;",
        "border-radius:8px;overflow:hidden;font-size:.88rem;margin-bottom:10px}",
        "th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #e2e5ea;vertical-align:top}",
        "th{background:#eef1f5;font-size:.72rem;text-transform:uppercase;color:#5c6570}",
        "tr:last-child td{border-bottom:0}",
        ".num{font-family:ui-monospace,monospace;font-weight:650;color:#1d4f91;white-space:nowrap}",
        ".meta{color:#5c6570;font-size:.85rem;margin:8px 0}",
        ".err{color:#b42318;font-weight:650}",
        "</style></head><body>",
        "<header><h1>{0}</h1>".format(_esc(title)),
        "<p>source: {0}</p></header><main>".format(_esc(run.get("source") or "")),
    ]
    for case in run.get("cases") or []:
        case_id = case.get("case") or "?"
        parts.append("<h2>Case {0}</h2>".format(_esc(case_id)))
        meta_bits = []
        if case.get("host_count_final") is not None:
            meta_bits.append("hosts={0}".format(case.get("host_count_final")))
        if case.get("osds_final") is not None:
            meta_bits.append("osds={0}".format(case.get("osds_final")))
        if case.get("agent_down_events") is not None:
            meta_bits.append(
                "agent_down_events={0}".format(case.get("agent_down_events"))
            )
        if case.get("error"):
            meta_bits.append(
                '<span class="err">error={0}</span>'.format(_esc(case.get("error")))
            )
        if meta_bits:
            parts.append('<p class="meta">{0}</p>'.format(" · ".join(meta_bits)))
        parts.append(
            "<table><thead><tr>"
            "<th>Metric</th><th>Seconds</th><th>Started</th><th>Finished</th><th>Meta</th>"
            "</tr></thead><tbody>"
        )
        for t in case.get("timings") or []:
            parts.append(
                "<tr><td>{0}</td><td class='num'>{1}</td><td>{2}</td><td>{3}</td><td><code>{4}</code></td></tr>".format(
                    _esc(t.get("name")),
                    _esc(_fmt_sec(t.get("seconds"))),
                    _esc(t.get("started_at")),
                    _esc(t.get("finished_at")),
                    _esc(json.dumps(t.get("meta") or {}, default=str)),
                )
            )
        parts.append("</tbody></table>")
        rss = case.get("mgr_rss")
        if rss:
            parts.append(
                '<p class="meta">mgr RSS: <code>{0}</code></p>'.format(
                    _esc(json.dumps(rss, default=str))
                )
            )
    parts.append("</main></body></html>")
    return "\n".join(parts)


def render_html_compare(
    run_a: Dict[str, Any],
    run_b: Dict[str, Any],
    label_a: str = "run A",
    label_b: str = "run B",
    title: str = "Cephadm agent scale timing compare",
) -> str:
    rows = compare_runs(run_a, run_b)
    parts = [
        "<!DOCTYPE html>",
        '<html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>{0}</title>".format(_esc(title)),
        "<style>",
        "body{font:15px/1.45 system-ui,sans-serif;margin:0;background:#f5f6f8;color:#1a1d23}",
        "header{background:#1a2332;color:#fff;padding:20px 24px}",
        "header h1{margin:0 0 4px;font-size:1.25rem}",
        "header p{margin:0;opacity:.85;font-size:.9rem}",
        "main{max-width:1200px;margin:0 auto;padding:18px 16px 40px}",
        "table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e2e5ea;",
        "border-radius:8px;overflow:hidden;font-size:.88rem}",
        "th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #e2e5ea}",
        "th{background:#eef1f5;font-size:.72rem;text-transform:uppercase;color:#5c6570}",
        ".num{font-family:ui-monospace,monospace;font-weight:650;white-space:nowrap}",
        ".worse{color:#b42318;font-weight:650}",
        ".better{color:#067647;font-weight:650}",
        ".meta{color:#5c6570;font-size:.85rem;margin:10px 0}",
        "</style></head><body>",
        "<header><h1>{0}</h1>".format(_esc(title)),
        "<p>{0} vs {1}</p></header><main>".format(_esc(label_a), _esc(label_b)),
        '<p class="meta">A: <code>{0}</code><br>B: <code>{1}</code></p>'.format(
            _esc(run_a.get("source")), _esc(run_b.get("source"))
        ),
        "<table><thead><tr>",
        "<th>Metric</th><th>{0} (s)</th><th>{1} (s)</th><th>Delta (s)</th><th>Delta %</th>".format(
            _esc(label_a), _esc(label_b)
        ),
        "</tr></thead><tbody>",
    ]
    for row in rows:
        delta = row.get("delta")
        cls = ""
        if isinstance(delta, (int, float)):
            if delta > 0:
                cls = "worse"
            elif delta < 0:
                cls = "better"
        pct = row.get("pct")
        pct_s = "—" if pct is None else "{0:+.1f}%".format(pct)
        delta_s = "—" if delta is None else "{0:+.2f}".format(delta)
        parts.append(
            "<tr><td>{0}</td><td class='num'>{1}</td><td class='num'>{2}</td>"
            "<td class='num {3}'>{4}</td><td class='num {3}'>{5}</td></tr>".format(
                _esc(row["metric"]),
                _esc(_fmt_sec(row.get("a"))),
                _esc(_fmt_sec(row.get("b"))),
                cls,
                _esc(delta_s),
                _esc(pct_s),
            )
        )
    parts.append("</tbody></table>")
    parts.append(
        '<p class="meta">Green delta = B faster; red = B slower.</p></main></body></html>'
    )
    return "\n".join(parts)


def write_html_report(log_dir: str, run: Optional[Dict[str, Any]] = None) -> str:
    run = run or load_run_timings(log_dir)
    run.setdefault("source", log_dir)
    path = os.path.join(log_dir, TIMINGS_HTML_NAME)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(render_html_single(run, title="Cephadm agent scale timings"))
    return path


def render_markdown_compare(
    run_a: Dict[str, Any],
    run_b: Dict[str, Any],
    label_a: str = "run A",
    label_b: str = "run B",
) -> str:
    rows = compare_runs(run_a, run_b)
    lines = [
        "# Cephadm agent scale timing compare",
        "",
        "- **{0}**: `{1}`".format(label_a, run_a.get("source")),
        "- **{0}**: `{1}`".format(label_b, run_b.get("source")),
        "",
        "| Metric | {0} (s) | {1} (s) | Delta (s) | Delta % |".format(label_a, label_b),
        "|---|---:|---:|---:|---:|",
    ]
    for row in rows:
        delta = row.get("delta")
        pct = row.get("pct")
        delta_s = "—" if delta is None else "{0:+.2f}".format(delta)
        pct_s = "—" if pct is None else "{0:+.1f}%".format(pct)
        lines.append(
            "| `{0}` | {1} | {2} | {3} | {4} |".format(
                row["metric"],
                _fmt_sec(row.get("a")),
                _fmt_sec(row.get("b")),
                delta_s,
                pct_s,
            )
        )
    lines.append("")
    return "\n".join(lines)


def publish_timings_to_share(log_dir: str, share: str) -> List[str]:
    """Copy timing JSON/HTML into an already-mounted share. Returns dest paths.

    The share is expected to be mounted by the caller (for example a CephFS
    export from the cluster under test). This helper only copies artifacts.
    """
    if not share or not os.path.isdir(log_dir):
        return []
    os.makedirs(share, exist_ok=True)
    written = []
    for name in sorted(os.listdir(log_dir)):
        if not name.startswith("agent_scale_timings"):
            continue
        src = os.path.join(log_dir, name)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(share, name)
        shutil.copy2(src, dst)
        written.append(dst)
    return written


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collate / compare cephadm agent scale regression timings"
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One log dir (or timings JSON) for a report; two for a compare",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output HTML path (default: <dir>/agent_scale_timings.html or compare next to first input)",
    )
    parser.add_argument(
        "--md",
        help="Also write markdown compare to this path (compare mode only)",
    )
    parser.add_argument("--label-a", default="run A")
    parser.add_argument("--label-b", default="run B")
    args = parser.parse_args(argv)

    if len(args.inputs) == 1:
        run = load_run_timings(args.inputs[0])
        out = args.output
        if not out:
            base = args.inputs[0]
            if os.path.isdir(base):
                out = os.path.join(base, TIMINGS_HTML_NAME)
            else:
                out = os.path.splitext(base)[0] + ".html"
        with open(out, "w", encoding="utf-8") as fh:
            fh.write(render_html_single(run))
        print("Wrote {0}".format(out))
        return 0

    if len(args.inputs) == 2:
        run_a = load_run_timings(args.inputs[0])
        run_b = load_run_timings(args.inputs[1])
        html_body = render_html_compare(
            run_a, run_b, label_a=args.label_a, label_b=args.label_b
        )
        out = args.output
        if not out:
            parent = (
                args.inputs[0]
                if os.path.isdir(args.inputs[0])
                else os.path.dirname(args.inputs[0]) or "."
            )
            out = os.path.join(parent, "agent_scale_timings_compare.html")
        with open(out, "w", encoding="utf-8") as fh:
            fh.write(html_body)
        print("Wrote {0}".format(out))
        if args.md:
            with open(args.md, "w", encoding="utf-8") as fh:
                fh.write(
                    render_markdown_compare(
                        run_a, run_b, label_a=args.label_a, label_b=args.label_b
                    )
                )
            print("Wrote {0}".format(args.md))
        else:
            # always print a short markdown table to stdout for quick paste
            sys.stdout.write(
                render_markdown_compare(
                    run_a, run_b, label_a=args.label_a, label_b=args.label_b
                )
            )
        return 0

    parser.error("Provide one input (report) or two inputs (compare)")
    return 2


if __name__ == "__main__":
    sys.exit(main())
