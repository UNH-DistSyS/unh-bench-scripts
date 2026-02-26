#!/usr/bin/env python3
"""Plot latency-target throughput search history for a single slowdown step."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import List, Dict


def _load_matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        return plt
    except Exception as exc:  # pragma: no cover
        sys.stderr.write(f"Plotting skipped (matplotlib unavailable): {exc}\n")
        return None


def read_rows(path: Path) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                rows.append(
                    {
                        "target_throughput": float(row["target_throughput"]),
                        "avg_throughput": float(row["avg_throughput"]),
                        "avg_latency_us": float(row["avg_latency_us"]),
                        "latency_target_us": float(row["latency_target_us"]),
                        "passed": 1.0 if str(row["passed"]).strip().lower() == "true" else 0.0,
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"Bad row in {path}: {row} ({exc})") from exc

    if not rows:
        raise ValueError(f"No rows found in {path}")

    return sorted(rows, key=lambda r: r["target_throughput"])


def plot_throughput(rows: List[Dict[str, float]], out_path: Path, title_prefix: str) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x = [r["target_throughput"] for r in rows]
    y = [r["avg_throughput"] for r in rows]
    colors = ["#1d4ed8" if r["passed"] > 0 else "#dc2626" for r in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(x, y, color="#0f172a", linewidth=1.5, alpha=0.65)
    plt.scatter(x, y, c=colors, s=42)
    plt.xlabel("Target throughput (ops/s)")
    plt.ylabel("Observed throughput (ops/s)")
    plt.title(f"{title_prefix} Throughput vs Search Target")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(out_path, bbox_inches="tight")
    plt.close("all")


def plot_latency(rows: List[Dict[str, float]], out_path: Path, title_prefix: str) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x = [r["target_throughput"] for r in rows]
    y_ms = [r["avg_latency_us"] / 1000.0 for r in rows]
    target_ms = rows[0]["latency_target_us"] / 1000.0
    colors = ["#1d4ed8" if r["passed"] > 0 else "#dc2626" for r in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(x, y_ms, color="#0f172a", linewidth=1.5, alpha=0.65)
    plt.scatter(x, y_ms, c=colors, s=42)
    plt.axhline(target_ms, color="#ea580c", linestyle="--", linewidth=1.5, label=f"Target {target_ms:.2f} ms")
    plt.xlabel("Target throughput (ops/s)")
    plt.ylabel("Average latency (ms)")
    plt.title(f"{title_prefix} Latency vs Search Target")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(out_path, bbox_inches="tight")
    plt.close("all")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", required=True, help="search_history.csv path")
    parser.add_argument("--throughput-plot-out", required=True, help="Output PNG for throughput plot")
    parser.add_argument("--latency-plot-out", required=True, help="Output PNG for latency plot")
    parser.add_argument("--title-prefix", default="Cassandra", help="Prefix for plot titles")
    args = parser.parse_args(argv)

    in_csv = Path(args.input_csv)
    thr_out = Path(args.throughput_plot_out)
    lat_out = Path(args.latency_plot_out)

    try:
        rows = read_rows(in_csv)
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1

    thr_out.parent.mkdir(parents=True, exist_ok=True)
    lat_out.parent.mkdir(parents=True, exist_ok=True)

    plot_throughput(rows, thr_out, args.title_prefix)
    plot_latency(rows, lat_out, args.title_prefix)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
