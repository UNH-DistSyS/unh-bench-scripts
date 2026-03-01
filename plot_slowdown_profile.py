#!/usr/bin/env python3
"""Plot fixed-target slowdown profile outputs."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List


def _load_matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        return plt
    except Exception as exc:  # pragma: no cover
        sys.stderr.write(f"Plotting skipped (matplotlib unavailable): {exc}\n")
        return None


@dataclass
class Row:
    step_value_raw: str
    step_label: str
    throughput_ops_s: float
    mean_latency_us: float
    median_latency_us: float
    p99_latency_us: float


def parse_step_numeric(mode: str, step_value: str) -> float:
    if mode in {"cpu", "memory", "network_bandwidth"}:
        return float(step_value)

    v = step_value.strip().lower()
    if v.endswith("us"):
        return float(v[:-2])
    if v.endswith("ms"):
        return float(v[:-2]) * 1000.0
    if v.endswith("s"):
        return float(v[:-1]) * 1_000_000.0
    return float(v)


def mode_xlabel(mode: str) -> str:
    return {
        "cpu": "CPU cores",
        "memory": "Memory limit (MB)",
        "network_bandwidth": "Network bandwidth (Mbps)",
        "network_latency": "Added network latency",
    }.get(mode, mode)


def mode_title(mode: str) -> str:
    return {
        "cpu": "CPU Degradation",
        "memory": "Memory Degradation",
        "network_bandwidth": "Network Bandwidth Degradation",
        "network_latency": "Network Latency Degradation",
    }.get(mode, mode)


def read_rows(path: Path) -> List[Row]:
    rows: List[Row] = []
    with path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                rows.append(
                    Row(
                        step_value_raw=str(row["step_value"]),
                        step_label=str(row["step_label"]),
                        throughput_ops_s=float(row["observed_throughput_ops_s"]),
                        mean_latency_us=float(row["mean_latency_us"]),
                        median_latency_us=float(row["median_latency_us"]),
                        p99_latency_us=float(row["p99_latency_us"]),
                    )
                )
            except (KeyError, ValueError) as exc:
                raise ValueError(f"Bad row in {path}: {row} ({exc})") from exc
    return rows


def ordered_rows(rows: List[Row], mode: str) -> List[Row]:
    return sorted(
        rows,
        key=lambda r: parse_step_numeric(mode, r.step_value_raw),
        reverse=mode in {"cpu", "memory", "network_bandwidth"},
    )


def plot_throughput(rows: List[Row], mode: str, out_path: Path) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x_labels = [r.step_label for r in rows]
    y = [r.throughput_ops_s for r in rows]

    plt.figure(figsize=(10, 6))
    plt.plot(range(len(x_labels)), y, marker="o", linewidth=2.0, color="#0f172a")
    plt.xticks(range(len(x_labels)), x_labels)
    plt.xlabel(mode_xlabel(mode))
    plt.ylabel("Observed throughput (ops/s)")
    plt.title(f"Observed Throughput vs {mode_title(mode)}")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, bbox_inches="tight")
    plt.close("all")


def plot_latency(rows: List[Row], mode: str, out_path: Path) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    x_labels = [r.step_label for r in rows]
    mean_ms = [r.mean_latency_us / 1000.0 for r in rows]
    median_ms = [r.median_latency_us / 1000.0 for r in rows]
    p99_ms = [r.p99_latency_us / 1000.0 for r in rows]

    plt.figure(figsize=(10, 6))
    plt.plot(range(len(x_labels)), mean_ms, marker="o", linewidth=2.0, label="Mean latency")
    plt.plot(range(len(x_labels)), median_ms, marker="o", linewidth=2.0, label="Median latency")
    plt.plot(range(len(x_labels)), p99_ms, marker="o", linewidth=2.0, label="P99 latency")
    plt.xticks(range(len(x_labels)), x_labels)
    plt.xlabel(mode_xlabel(mode))
    plt.ylabel("Latency (ms)")
    plt.title(f"Latency Metrics vs {mode_title(mode)}")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.legend(loc="best")
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, bbox_inches="tight")
    plt.close("all")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--mode", required=True, choices=["cpu", "memory", "network_bandwidth", "network_latency"])
    parser.add_argument("--throughput-plot-out", required=True)
    parser.add_argument("--latency-plot-out", required=True)
    args = parser.parse_args(argv)

    in_csv = Path(args.input_csv)
    rows = read_rows(in_csv)
    if not rows:
        sys.stderr.write(f"No rows found in {in_csv}\n")
        return 1

    rows = ordered_rows(rows, args.mode)
    plot_throughput(rows, args.mode, Path(args.throughput_plot_out))
    plot_latency(rows, args.mode, Path(args.latency_plot_out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
