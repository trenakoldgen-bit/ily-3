from __future__ import annotations

import json
import statistics
import time

from rich.console import Console
from rich.table import Table

from .cache import Cache


def _verdict(frees: int, median: float | None) -> tuple[str, str]:
    if frees >= 3 and median is not None and median < 1800:
        return "hot", "bold green"
    if frees >= 1:
        return "warm", "yellow"
    return "cold", "dim"


def run(path: str, as_json: bool = False) -> None:
    cache = Cache(path, [], [])
    now = time.time()
    data = []

    for row in cache.churn():
        stamps = cache.free_timestamps(row["code"])
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        median = statistics.median(gaps) if gaps else None
        verdict, _ = _verdict(row["frees"], median)
        data.append({
            "code": row["code"],
            "state": row["state"],
            "checks": row["checks"],
            "frees": row["frees"],
            "median_gap_s": round(median, 1) if median else None,
            "last_free_s_ago": round(now - row["last_free"], 1) if row["last_free"] else None,
            "verdict": verdict,
        })

    if as_json:
        print(json.dumps(data, indent=2))
        return

    table = Table(title=f"churn report — {len(data)} code(s)", box=None)
    for column in ("code", "state", "checks", "frees", "median gap", "last free", "verdict"):
        table.add_column(column)
    for item in data:
        _, style = _verdict(item["frees"], item["median_gap_s"])
        table.add_row(item["code"], item["state"], str(item["checks"]), str(item["frees"]),
                      f"{item['median_gap_s']}s" if item["median_gap_s"] else "—",
                      f"{item['last_free_s_ago']}s ago" if item["last_free_s_ago"] else "—",
                      f"[{style}]{item['verdict']}[/]")
    Console().print(table)