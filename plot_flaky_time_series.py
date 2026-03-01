#!/usr/bin/env python3
"""Build aggregated flaky time-series CSV and graphs."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


def _load_matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        return plt
    except Exception as exc:  # pragma: no cover
        sys.stderr.write(f"Plotting skipped (matplotlib unavailable): {exc}\n")
        return None


def read_samples(path: Path) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                {
                    "time_sec": float(row["time_sec"]),
                    "throughput_ops_s": float(row["throughput_ops_s"]),
                    "mean_latency_us": float(row["mean_latency_us"]),
                    "median_latency_us": float(row["median_latency_us"]),
                }
            )
    return rows


def write_averages(rows: List[Dict[str, float]], out_csv: Path) -> List[Dict[str, float]]:
    grouped: Dict[int, List[Dict[str, float]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["time_sec"])].append(row)

    averaged: List[Dict[str, float]] = []
    for t in sorted(grouped.keys()):
        bucket = grouped[t]
        n = len(bucket)
        averaged.append(
            {
                "time_sec": float(t),
                "avg_throughput_ops_s": sum(r["throughput_ops_s"] for r in bucket) / n,
                "avg_mean_latency_us": sum(r["mean_latency_us"] for r in bucket) / n,
                "avg_median_latency_us": sum(r["median_latency_us"] for r in bucket) / n,
                "sample_count": float(n),
            }
        )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["time_sec", "avg_throughput_ops_s", "avg_mean_latency_us", "avg_median_latency_us", "sample_count"])
        for row in averaged:
            writer.writerow(
                [
                    int(row["time_sec"]),
                    f"{row['avg_throughput_ops_s']:.4f}",
                    f"{row['avg_mean_latency_us']:.4f}",
                    f"{row['avg_median_latency_us']:.4f}",
                    int(row["sample_count"]),
                ]
            )
    return averaged


def plot_throughput(averaged: List[Dict[str, float]], out_png: Path) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x = [r["time_sec"] for r in averaged]
    y = [r["avg_throughput_ops_s"] for r in averaged]
    plt.figure(figsize=(10, 5))
    plt.plot(x, y, linewidth=2.0, color="#0f172a")
    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (ops/s)")
    plt.title("CPU Flaky Throughput Over Time")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, bbox_inches="tight")
    plt.close("all")


def plot_latency(averaged: List[Dict[str, float]], out_png: Path) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x = [r["time_sec"] for r in averaged]
    y_mean = [r["avg_mean_latency_us"] / 1000.0 for r in averaged]
    y_median = [r["avg_median_latency_us"] / 1000.0 for r in averaged]
    plt.figure(figsize=(10, 5))
    plt.plot(x, y_mean, linewidth=2.0, label="Mean latency")
    plt.plot(x, y_median, linewidth=2.0, label="Median latency")
    plt.xlabel("Time (s)")
    plt.ylabel("Latency (ms)")
    plt.title("CPU Flaky Latency Over Time")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.legend(loc="best")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, bbox_inches="tight")
    plt.close("all")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-samples-csv", required=True)
    parser.add_argument("--output-avg-csv", required=True)
    parser.add_argument("--throughput-plot-out", required=True)
    parser.add_argument("--latency-plot-out", required=True)
    args = parser.parse_args(argv)

    samples = read_samples(Path(args.input_samples_csv))
    if not samples:
        sys.stderr.write("No sample rows found\n")
        return 1

    averaged = write_averages(samples, Path(args.output_avg_csv))
    plot_throughput(averaged, Path(args.throughput_plot_out))
    plot_latency(averaged, Path(args.latency_plot_out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
