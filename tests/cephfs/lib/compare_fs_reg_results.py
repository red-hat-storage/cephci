#!/usr/bin/env python3
"""
Compare regression results across any number of result roots (RH/IBM/etc.).

Parses each suite folder for index.html (preferred) or xunit.xml (fallback),
collects per-test statuses, and generates a comparison report including:
- Dynamic multi-root "failures_comparison.csv"
  -> Suite, Test, <Label1_Status(ceph_ver)>, <Label2_Status(ceph_ver)>, ...
- Per-root platform summary and per-suite breakdown
- Markdown report with versions and unique/common failure sets

USAGE:
  python compare_regression_failures.py ROOT1 ROOT2 [ROOT3 ...] [-o /path/to/output_dir]

OUTPUTS in -o directory (default "."):
  - report.md
  - results_<idx>.json                  # {suite: {test_name: STATUS}} for each root
  - failures_comparison.csv             # Suite, Test, <Label1>, <Label2>, ...
  - platform_summary.csv                # one row per root (Label/Vendor/Build/Ceph/Distro + totals)
  - suite_summary.csv                   # per-suite totals per root

STATUS normalisation:
  - PASS: "pass", "success"
  - FAIL: "fail", "failed", "failure", "error"
  - NOT_RUN: "skip", "skipped", "not run", "blocked", "aborted", "na", "n/a"
  - Any unknown non-empty status is kept as-is for transparency.

 Note : Should be executed on Magna002
"""

import argparse
import csv
import json
import os
import re
from typing import Dict, List, Optional, Set, Tuple

# Optional BeautifulSoup for robust HTML parsing
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None


def find_files(root: str, filename: str) -> List[str]:
    paths = []
    for dirpath, _dirnames, filenames in os.walk(root):
        if filename in filenames:
            paths.append(os.path.join(dirpath, filename))
    return paths


def normalise_status(s: str) -> str:
    s_clean = (s or "").strip().lower()
    if s_clean in {"pass", "passed", "success"}:
        return "PASS"
    if s_clean in {"fail", "failed", "failure", "error"}:
        return "FAIL"
    if any(
        k in s_clean
        for k in ["skip", "skipped", "not run", "blocked", "abort", "n/a", "na"]
    ):
        return "NOT_RUN"
    return s.strip().upper() if s.strip() else ""


def parse_index_html_results(path: str) -> List[Tuple[str, str]]:
    """Return list of (test_name, status) from 'Test Results' table."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
    except Exception:
        return []

    rows_out: List[Tuple[str, str]] = []

    if BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(html, "html.parser")
            # locate table under "Test Results"
            table = None
            for h in soup.find_all(["h2", "h3"]):
                if h.get_text(strip=True).lower() == "test results":
                    table = h.find_next("table")
                    if table:
                        break
            if table is None:
                # fallback: any table with headers "Test Name" & "Status"
                for t in soup.find_all("table"):
                    head = t.find("tr")
                    if not head:
                        continue
                    headers = [
                        th.get_text(strip=True).lower()
                        for th in head.find_all(["th", "td"])
                    ]
                    if "test name" in headers and "status" in headers:
                        table = t
                        break
            if table is None:
                return []

            rows = table.find_all("tr")[1:]  # skip header
            for tr in rows:
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue
                test_name = tds[0].get_text(strip=True)
                status_text = tds[3].get_text(strip=True)
                status = normalise_status(status_text)
                if test_name:
                    rows_out.append((test_name, status))
            return rows_out
        except Exception:
            pass

    # Regex fallback
    row_pattern = re.compile(r"<tr>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    cell_pattern = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)

    rows = row_pattern.findall(html)
    for row in rows[1:]:  # skip header
        cells = cell_pattern.findall(row)
        if len(cells) >= 4:

            def strip_tags(s: str) -> str:
                return re.sub(r"<[^>]+>", "", s).strip()

            test_name = strip_tags(cells[0])
            status_text = strip_tags(cells[3])
            status = normalise_status(status_text)
            if test_name:
                rows_out.append((test_name, status))
    return rows_out


def parse_xunit_results(path: str) -> List[Tuple[str, str]]:
    """Parse JUnit XML to collect testcase names and PASS/FAIL/NOT_RUN."""
    try:
        import xml.etree.ElementTree as ET

        tree = ET.parse(path)
        root = tree.getroot()
        results: List[Tuple[str, str]] = []
        for tc in root.iter():
            if tc.tag.endswith("testcase"):
                name = tc.attrib.get("name") or ""
                if not name:
                    continue
                status = "PASS"
                has_failure = False
                has_error = False
                has_skip = False
                for child in list(tc):
                    tag = child.tag.lower()
                    if tag.endswith("failure"):
                        has_failure = True
                    elif tag.endswith("error"):
                        has_error = True
                    elif tag.endswith("skipped") or tag.endswith("skip"):
                        has_skip = True
                if has_failure or has_error:
                    status = "FAIL"
                elif has_skip:
                    status = "NOT_RUN"
                else:
                    status = "PASS"
                results.append((name, status))
        return results
    except Exception:
        return []


def collect_results(root: str) -> Dict[str, Dict[str, str]]:
    """
    Walk `root`, parse index.html and/or xunit.xml.
    Return: {suite_rel_path: {test_name: STATUS}}.
    """
    index_paths = find_files(root, "index.html")
    xunit_paths = find_files(root, "xunit.xml")

    suite_dirs: Dict[str, Dict[str, Optional[str]]] = {}
    for p in index_paths:
        suite_dirs[os.path.dirname(p)] = {"index": p, "xunit": None}
    for p in xunit_paths:
        d = os.path.dirname(p)
        meta = suite_dirs.setdefault(d, {"index": None, "xunit": None})
        meta["xunit"] = p

    results_by_suite: Dict[str, Dict[str, str]] = {}

    for suite_dir, meta in suite_dirs.items():
        rel = os.path.relpath(suite_dir, root)
        tests: Dict[str, str] = {}
        if meta.get("xunit"):
            for name, status in parse_xunit_results(meta["xunit"]):  # type: ignore
                tests[name] = status
        if meta.get("index"):
            for name, status in parse_index_html_results(meta["index"]):  # type: ignore
                tests[name] = status
        if tests:
            results_by_suite[rel] = tests

    return results_by_suite


def flatten_status(
    results_by_suite: Dict[str, Dict[str, str]], target: str
) -> Set[str]:
    acc: Set[str] = set()
    for tests in results_by_suite.values():
        for name, status in tests.items():
            if status == target:
                acc.add(name)
    return acc


def write_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def write_csv(path: str, rows: List[List[str]], header: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def extract_version_info(path: str) -> Dict[str, Optional[str]]:
    """
    Best-effort parse of vendor/build/distro/ceph version from a results root path.
    Expected patterns include:
      .../(RH|IBM)/<build>/rhel-9/(Regression|Upgrade)/<ceph_version>/...
    Returns: {"vendor": "RH"|"IBM"|None, "build": "...", "ceph": "...", "distro": "..."}
    """
    vendor = None
    build = None
    ceph = None
    distro = None

    parts = [p for p in (path or "").split("/") if p]

    for i, p in enumerate(parts):
        if p in ("RH", "IBM"):
            vendor = p
            if i + 1 < len(parts):
                nxt = parts[i + 1]
                if re.match(r"^\d+(?:\.\d+)*$", nxt):  # e.g., 8.1
                    build = nxt

        if p.lower().startswith("rhel-"):
            distro = p

        if p in ("Regression", "Upgrade") and i + 1 < len(parts):
            nxt = parts[i + 1]
            # e.g., 19.2.1-245
            if re.match(r"^\d+(?:\.\d+){1,2}(?:-\d+)?$", nxt):
                ceph = nxt

    return {"vendor": vendor, "build": build, "ceph": ceph, "distro": distro}


def make_label(info: Dict[str, Optional[str]], idx: int) -> str:
    """Label used as CSV column header: <Vendor>_Status(<Ceph>) or R<idx>_Status(n/a)"""
    vendor = info.get("vendor") or f"R{idx+1}"
    ceph = info.get("ceph") or "n/a"
    return f"{vendor}_Status({ceph})"


def make_markdown_report(
    roots: List[str],
    results_all: List[Dict[str, Dict[str, str]]],
) -> str:
    # Header w/ versions
    infos = [extract_version_info(r) for r in roots]

    def fmt_info(label: str, info: Dict[str, Optional[str]]) -> str:
        v = info.get("vendor") or label
        b = info.get("build") or "n/a"
        c = info.get("ceph") or "n/a"
        d = info.get("distro") or "n/a"
        return f"**{v}:** Build {b} · Ceph {c} · Distro {d}"

    lines: List[str] = []
    lines.append("# Regression Comparison (multi-root)\n")
    for i, r in enumerate(roots):
        lines.append(f"- `{r}`  ")
        lines.append(f"  - {fmt_info(f'R{i+1}', infos[i])}")
    lines.append("")

    # Common/unique failures across N roots
    failures_sets = [flatten_status(res, "FAIL") for res in results_all]
    if failures_sets:
        common_all = (
            set.intersection(*failures_sets)
            if len(failures_sets) > 1
            else failures_sets[0]
        )
    else:
        common_all = set()

    lines.append("## Common failures across all roots\n")
    if not common_all:
        lines.append("_None_\n")
    else:
        for t in sorted(common_all):
            lines.append(f"- {t}\n")
    lines.append("")

    # Unique per root
    lines.append("## Unique failures by root\n")
    if failures_sets:
        for i, s in enumerate(failures_sets):
            unique_i = (
                s - set().union(*(failures_sets[:i] + failures_sets[i + 1 :]))
                if len(failures_sets) > 1
                else s
            )
            label = make_label(infos[i], i)
            lines.append(f"### {label}\n")
            if not unique_i:
                lines.append("_None_\n")
            else:
                for t in sorted(unique_i):
                    lines.append(f"- {t}\n")
            lines.append("")
    else:
        lines.append("_No failures detected in any root_\n")

    # Per-suite breakdowns
    lines.append("## Per-suite breakdown (by root)\n")
    for i, res in enumerate(results_all):
        label = make_label(infos[i], i)
        lines.append(f"### {label}\n")
        if not res:
            lines.append("_No suites parsed_\n\n")
            continue
        for suite, tests in sorted(res.items()):
            total = len(tests)
            c_fail = sum(1 for s in tests.values() if s == "FAIL")
            c_nr = sum(1 for s in tests.values() if s == "NOT_RUN")
            lines.append(
                f"**{suite}** — Total: {total} | FAIL: {c_fail} | NOT_RUN: {c_nr}\n"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare regression results across multiple result roots."
    )
    parser.add_argument("roots", nargs="+", help="Paths to result roots (2 or more).")
    parser.add_argument(
        "-o", "--output-dir", default=".", help="Directory to write report files"
    )
    args = parser.parse_args()

    if len(args.roots) < 2:
        parser.error("Provide at least two result roots.")

    # Collect results for each root
    results_all: List[Dict[str, Dict[str, str]]] = []
    infos = []
    for idx, root in enumerate(args.roots):
        res = collect_results(root)
        results_all.append(res)
        infos.append(extract_version_info(root))
        # Write per-root JSON for debugging/inspection
        write_json(os.path.join(args.output_dir, f"results_{idx+1}.json"), res)

    # Build dynamic failures_comparison.csv
    # Columns: Suite, Test, <Label1>, <Label2>, ...
    headers = ["Suite", "Test"] + [
        make_label(infos[i], i) for i in range(len(args.roots))
    ]

    # Union of suites
    all_suites: Set[str] = (
        set().union(*[set(r.keys()) for r in results_all]) if results_all else set()
    )

    rows: List[List[str]] = []
    for suite in sorted(all_suites):
        # Union of tests across this suite for all roots
        all_tests: Set[str] = set()
        for res in results_all:
            all_tests |= set(res.get(suite, {}).keys())
        for t in sorted(all_tests):
            row = [suite, t]
            for res in results_all:
                row.append(res.get(suite, {}).get(t, ""))
            rows.append(row)

    write_csv(os.path.join(args.output_dir, "failures_comparison.csv"), rows, headers)

    # platform_summary.csv (per root totals)
    plat_rows: List[List[str]] = []
    plat_header = [
        "Label",
        "Vendor",
        "Build",
        "Ceph",
        "Distro",
        "Total_Tests",
        "Ran",
        "Pass",
        "Fail",
        "Not_Run",
    ]

    for i, res in enumerate(results_all):
        # Totals by summing per-suite counts (not de-duplicated by test name)
        total = sum(len(tests) for tests in res.values())
        pass_c = sum(
            sum(1 for s in tests.values() if s == "PASS") for tests in res.values()
        )
        fail_c = sum(
            sum(1 for s in tests.values() if s == "FAIL") for tests in res.values()
        )
        nr_c = sum(
            sum(1 for s in tests.values() if s == "NOT_RUN") for tests in res.values()
        )
        ran = pass_c + fail_c
        info = infos[i]
        plat_rows.append(
            [
                make_label(info, i),
                info.get("vendor") or "",
                info.get("build") or "",
                info.get("ceph") or "",
                info.get("distro") or "",
                total,
                ran,
                pass_c,
                fail_c,
                nr_c,
            ]
        )

    write_csv(
        os.path.join(args.output_dir, "platform_summary.csv"), plat_rows, plat_header
    )

    # suite_summary.csv (per suite per root)
    suite_rows: List[List[str]] = []
    suite_header = ["Label", "Suite", "Total_Tests", "Ran", "Pass", "Fail", "Not_Run"]

    for i, res in enumerate(results_all):
        label = make_label(infos[i], i)
        for suite, tests in sorted(res.items()):
            total = len(tests)
            pass_c = sum(1 for s in tests.values() if s == "PASS")
            fail_c = sum(1 for s in tests.values() if s == "FAIL")
            nr_c = sum(1 for s in tests.values() if s == "NOT_RUN")
            ran = pass_c + fail_c
            suite_rows.append([label, suite, total, ran, pass_c, fail_c, nr_c])

    write_csv(
        os.path.join(args.output_dir, "suite_summary.csv"), suite_rows, suite_header
    )

    # Markdown report
    md = make_markdown_report(args.roots, results_all)
    report_path = os.path.join(args.output_dir, "report.md")
    os.makedirs(args.output_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(md)
    print(f"\nWrote: {report_path}")
    for i in range(len(args.roots)):
        print(f"Wrote: {os.path.join(args.output_dir, f'results_{i+1}.json')}")
    print(f"Wrote: {os.path.join(args.output_dir, 'failures_comparison.csv')}")
    print(f"Wrote: {os.path.join(args.output_dir, 'platform_summary.csv')}")
    print(f"Wrote: {os.path.join(args.output_dir, 'suite_summary.csv')}")


if __name__ == "__main__":
    main()


# Helper to convert csv to xls
# import os, re, glob, pandas as pd
# with pd.ExcelWriter("combined.xlsx", engine="openpyxl") as xw:
#     for path in glob.glob("*.csv"):
#         name = re.sub(r'[:\\/*?\\[\\]]','_', os.path.splitext(os.path.basename(path))[0])[:31]
#         pd.read_csv(path).to_excel(xw, sheet_name=name, index=False)
# print("Wrote combined.xlsx")
