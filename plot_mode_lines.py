#!/usr/bin/env python3
"""Plot throughput vs degradation with one line per latency target for a slowdown mode."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


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
    mode: str
    latency_target_ms: float
    step_value_raw: str
    step_label: str
    observed_throughput: float


def parse_latency_target_from_dir(name: str) -> float:
    m = re.match(r"max_latency_target_(.+)$", name)
    if not m:
        raise ValueError(f"Could not parse latency target from directory '{name}'")
    return float(m.group(1))


def parse_step_numeric(mode: str, step_value: str) -> float:
    if mode in {"cpu", "memory", "network_bandwidth"}:
        return float(step_value)

    # network_latency mode
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


def read_rows(out_base: Path, mode: str) -> List[Row]:
    rows: List[Row] = []

    for summary in sorted(out_base.glob(f"max_latency_target_*/{mode}_summary.csv")):
        latency_target_ms = parse_latency_target_from_dir(summary.parent.name)
        with summary.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    observed = row.get("observed_throughput_at_max", row.get("max_target_throughput", "0"))
                    rows.append(
                        Row(
                            mode=mode,
                            latency_target_ms=latency_target_ms,
                            step_value_raw=str(row["step_value"]),
                            step_label=str(row["step_label"]),
                            observed_throughput=float(observed),
                        )
                    )
                except (KeyError, ValueError) as exc:
                    raise ValueError(f"Bad row in {summary}: {row} ({exc})") from exc

    return rows


def step_order(rows: List[Row], mode: str) -> List[Tuple[str, str]]:
    unique: Dict[str, Tuple[str, float]] = {}
    for row in rows:
        key = row.step_label
        if key not in unique:
            unique[key] = (row.step_value_raw, parse_step_numeric(mode, row.step_value_raw))

    items = [(label, val[0], val[1]) for label, val in unique.items()]

    reverse = mode in {"cpu", "memory", "network_bandwidth"}
    items.sort(key=lambda x: x[2], reverse=reverse)

    return [(label, raw) for label, raw, _ in items]


def write_merged_csv(rows: List[Row], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["mode", "latency_target_ms", "step_value", "step_label", "observed_throughput_ops_s"])
        for row in sorted(rows, key=lambda r: (r.latency_target_ms, r.step_label)):
            writer.writerow([
                row.mode,
                f"{row.latency_target_ms:.4f}",
                row.step_value_raw,
                row.step_label,
                f"{row.observed_throughput:.4f}",
            ])


def plot_lines(rows: List[Row], mode: str, out_png: Path) -> None:
    plt = _load_matplotlib()
    if plt is None:
        return

    order = step_order(rows, mode)
    if not order:
        return

    labels = [label for label, _ in order]

    by_target: Dict[float, Dict[str, float]] = {}
    for row in rows:
        by_target.setdefault(row.latency_target_ms, {})[row.step_label] = row.observed_throughput

    targets = sorted(by_target.keys())
    plt.figure(figsize=(10, 6))
    for idx, latency_target in enumerate(targets, start=1):
        y = [by_target[latency_target].get(label, float("nan")) for label in labels]
        line_label = f"Series {idx}" if len(targets) > 1 else None
        plt.plot(range(len(labels)), y, marker="o", linewidth=2.0, label=line_label)

    plt.xticks(range(len(labels)), labels)
    plt.xlabel(mode_xlabel(mode))
    plt.ylabel("Observed throughput (ops/s)")
    plt.title(f"Throughput vs {mode_title(mode)}")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    if len(targets) > 1:
        plt.legend(loc="best")
    plt.tight_layout()

    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, bbox_inches="tight")
    plt.close("all")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-base", required=True, help="Root output directory")
    parser.add_argument("--mode", required=True, choices=["cpu", "memory", "network_bandwidth", "network_latency"])
    parser.add_argument("--output-png", required=True)
    parser.add_argument("--output-csv", required=True)
    args = parser.parse_args(argv)

    out_base = Path(args.out_base)
    rows = read_rows(out_base, args.mode)
    if not rows:
        sys.stderr.write(f"No summaries found for mode '{args.mode}' under {out_base}\n")
        return 1

    write_merged_csv(rows, Path(args.output_csv))
    plot_lines(rows, args.mode, Path(args.output_png))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
