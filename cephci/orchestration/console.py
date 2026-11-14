"""Human readable summary of a run, printed at the end of the execution."""

from typing import Dict, List

HEADER = (
    "\n{name:<30s}   {desc:<60s}   {duration:<30s}   {status:<15s}    {comments:>15s}"
)
ROW = "{name:<30.30s}   {desc:<60.60s}   {duration:<30s}   {status:<15s}   {comments:>15s}"


def print_results(records: List[Dict]) -> None:
    """Print the result table of a run.

    Args:
        records (list): The result record of every test that ran.

    Returns:
        None
    """
    print(
        HEADER.format(
            name="TEST NAME",
            desc="TEST DESCRIPTION",
            duration="DURATION",
            status="STATUS",
            comments="COMMENTS",
        )
    )

    for record in records:
        print(
            ROW.format(
                name=record["name"],
                desc=record["desc"] or "None",
                duration=str(record["duration"]) if record.get("duration") else "0s",
                status=record["status"],
                comments=record["comments"],
            )
        )
