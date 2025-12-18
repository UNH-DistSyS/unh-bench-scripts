#!/usr/bin/env python3
"""Process Cassandra bench data points passed in as CLI arguments and plot them.

Each argument must look like: cpu_pct,avg_throughput,avg_latency
Example: 0,100000.0,620.0 25,82000.0,640.0

Outputs a CSV table to stdout by default, can also write JSON, and produces
a bar chart (normalized to the highest throughput = 100%) unless disabled.
"""

import argparse
import csv
import json
import sys
from typing import List, Optional, Dict, Any


def parse_point(text: str) -> Dict[str, Optional[float]]:
    parts = text.split(",")
    if len(parts) != 3:
        raise ValueError(f"Bad point '{text}': expected cpu,throughput,latency")

    def parse_num(val: str) -> Optional[float]:
        val = val.strip()
        if not val or val.upper() == "N/A":
            return None
        try:
            return float(val)
        except ValueError:
            return None

    cpu = parse_num(parts[0])
    thr = parse_num(parts[1])
    lat = parse_num(parts[2])
    return {"cpu_pct": cpu, "throughput": thr, "latency": lat}


def add_normalized(points: List[Dict[str, Optional[float]]]) -> None:
    max_thr = None
    for pt in points:
        thr = pt.get("throughput")
        if thr is None:
            continue
        if max_thr is None or thr > max_thr:
            max_thr = thr

    for pt in points:
        thr = pt.get("throughput")
        if max_thr and thr is not None:
            pt["normalized_throughput"] = thr / max_thr
        else:
            pt["normalized_throughput"] = None


def write_csv(points: List[Dict[str, Optional[float]]], fh) -> None:
    writer = csv.writer(fh)
    writer.writerow(
        ["cpu_pct", "cpu_availability_pct", "throughput", "latency", "normalized_throughput"]
    )
    for pt in points:
        avail = None if pt["cpu_pct"] is None else 100 - pt["cpu_pct"]
        writer.writerow(
            [
                "" if pt["cpu_pct"] is None else pt["cpu_pct"],
                "" if avail is None else avail,
                "" if pt["throughput"] is None else f"{pt['throughput']:.2f}",
                "" if pt["latency"] is None else f"{pt['latency']:.2f}",
                ""
                if pt.get("normalized_throughput") is None
                else f"{pt['normalized_throughput']:.4f}",
            ]
        )


def print_table(points: List[Dict[str, Optional[float]]]) -> None:
    header = ("CPU avail", "Throughput", "Latency", "NormThr")
    widths = [12, 14, 12, 10]
    fmt = f"{{:{widths[0]}s}} {{:{widths[1]}s}} {{:{widths[2]}s}} {{:{widths[3]}s}}"
    print(fmt.format(*header))
    print("-" * sum(widths))
    for pt in points:
        cpu_avail = ""
        if pt["cpu_pct"] is not None:
            cpu_avail = f"{100 - pt['cpu_pct']:.0f}%"
        thr = "" if pt["throughput"] is None else f"{pt['throughput']:.2f}"
        lat = "" if pt["latency"] is None else f"{pt['latency']:.2f}"
        norm = (
            "" if pt.get("normalized_throughput") is None else f"{pt['normalized_throughput']:.4f}"
        )
        print(fmt.format(cpu_avail, thr, lat, norm))


def plot(points: List[Dict[str, Optional[float]]], out_path: str) -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        sys.stderr.write(f"Plotting skipped (matplotlib unavailable): {exc}\n")
        return False

    # Sort by CPU availability (descending) for a natural left-to-right view.
    def sort_key(pt):
        return pt["cpu_pct"] if pt["cpu_pct"] is not None else 999

    pts_sorted = sorted(points, key=sort_key)

    labels = []
    heights = []
    for pt in pts_sorted:
        if pt["cpu_pct"] is None:
            labels.append("?")
        else:
            labels.append(f"{100 - pt['cpu_pct']:.0f}%")
        norm = pt.get("normalized_throughput")
        heights.append(0 if norm is None else norm * 100.0)

    if not labels:
        return False

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, heights, color="steelblue")
    plt.xlabel("CPU availability (100% - slowdown)")
    plt.ylabel("Normalized throughput (max = 100%)")
    plt.ylim(0, max(heights + [100]) * 1.1)
    plt.title("Cassandra throughput vs CPU slowdown")
    plt.grid(axis="y", linestyle="--", alpha=0.6)

    for bar, height in zip(bars, heights):
        if height <= 0:
            label = "n/a"
            y = 1
        else:
            label = f"{height:.1f}%"
            y = height + max(2, 0.01 * height)
        plt.text(bar.get_x() + bar.get_width() / 2, y, label, ha="center", va="bottom", fontsize=8)

    try:
        plt.tight_layout()
        plt.savefig(out_path, bbox_inches="tight")
        sys.stdout.write(f"Wrote plot to {out_path}\n")
        return True
    finally:
        plt.close("all")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "points",
        nargs="+",
        help="Data points formatted as cpu_pct,avg_throughput,avg_latency (N/A allowed)",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "json", "table"),
        default="csv",
        help="Output format for stdout (default: csv)",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional path to also write a CSV file with parsed data",
    )
    parser.add_argument(
        "--plot-out",
        default="cass_normalized_throughput.png",
        help="Path to write bar plot (default: cass_normalized_throughput.png)",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Disable plotting even if matplotlib is available",
    )
    args = parser.parse_args(argv)

    try:
        points = [parse_point(p) for p in args.points]
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1

    add_normalized(points)

    if args.output_csv:
        try:
            with open(args.output_csv, "w", newline="") as fh:
                write_csv(points, fh)
        except OSError as exc:
            sys.stderr.write(f"Failed to write CSV to {args.output_csv}: {exc}\n")
            return 1

    if args.format == "csv":
        write_csv(points, sys.stdout)
    elif args.format == "json":
        # Include normalized throughput in JSON as well.
        sys.stdout.write(json.dumps(points, indent=2))
        if not sys.stdout.isatty():
            sys.stdout.write("\n")
    else:  # table
        print_table(points)

    if not args.no_plot and args.plot_out:
        plot(points, args.plot_out)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
