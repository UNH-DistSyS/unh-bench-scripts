#!/usr/bin/env python3
"""Process Cassandra bench data points passed in as CLI arguments and plot them.

Each argument must look like: cpu_pct,avg_throughput,avg_latency[,label]
Example: 0,100000.0,620.0,8_cores 25,82000.0,640.0,6_cores

Outputs a CSV table to stdout by default, can also write JSON, and produces
normalized throughput, raw throughput, and raw latency plots unless disabled.
"""

import argparse
import csv
import json
import sys
from typing import List, Optional, Dict, Any


def parse_point(text: str) -> Dict[str, Optional[float]]:
    parts = text.split(",")
    if len(parts) not in (3, 4):
        raise ValueError(f"Bad point '{text}': expected cpu,throughput,latency[,label]")

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
    label = None
    if len(parts) == 4:
        label = parts[3].strip() or None
    return {"cpu_pct": cpu, "throughput": thr, "latency": lat, "label": label}


def add_normalized(points: List[Dict[str, Optional[float]]]) -> None:
    max_thr = None
    min_lat = None
    for pt in points:
        thr = pt.get("throughput")
        lat = pt.get("latency")
        if thr is None:
            pass
        elif max_thr is None or thr > max_thr:
            max_thr = thr
        if lat is None:
            continue
        if min_lat is None or lat < min_lat:
            min_lat = lat

    for pt in points:
        thr = pt.get("throughput")
        lat = pt.get("latency")
        if max_thr and thr is not None:
            pt["normalized_throughput"] = thr / max_thr
            pt["throughput_slowdown_pct"] = (1.0 - (thr / max_thr)) * 100.0
        else:
            pt["normalized_throughput"] = None
            pt["throughput_slowdown_pct"] = None
        if min_lat and lat is not None:
            pt["normalized_latency"] = lat / min_lat
            pt["latency_increase_pct"] = ((lat / min_lat) - 1.0) * 100.0
        else:
            pt["normalized_latency"] = None
            pt["latency_increase_pct"] = None


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


def _load_matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        return plt
    except Exception as exc:
        sys.stderr.write(f"Plotting skipped (matplotlib unavailable): {exc}\n")
        return None


def _sort_points(points: List[Dict[str, Optional[float]]]) -> List[Dict[str, Optional[float]]]:
    def sort_key(pt):
        return pt["cpu_pct"] if pt["cpu_pct"] is not None else 999

    return sorted(points, key=sort_key)


def _labels_for_points(points: List[Dict[str, Optional[float]]]) -> List[str]:
    labels = []
    for pt in points:
        label = pt.get("label")
        if label:
            labels.append(str(label))
        elif pt["cpu_pct"] is None:
            labels.append("?")
        else:
            labels.append(f"{pt['cpu_pct']:.0f}%")
    return labels


def plot_normalized(points: List[Dict[str, Optional[float]]], out_path: str) -> bool:
    plt = _load_matplotlib()
    if plt is None:
        return False

    pts_sorted = _sort_points(points)
    labels = _labels_for_points(pts_sorted)

    heights = []
    for pt in pts_sorted:
        norm = pt.get("normalized_throughput")
        heights.append(0 if norm is None else norm * 100.0)

    if not labels:
        return False

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, heights, color="#2f6f9f")
    plt.xlabel("Slowdown (%)")
    plt.ylabel("Normalized throughput (max = 100%)")
    plt.ylim(0, max(heights + [100]) * 1.1)
    plt.title("Cassandra Normalized Throughput vs Slowdown")
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


def _plot_series(
    points: List[Dict[str, Optional[float]]],
    y_key: str,
    y_label: str,
    title: str,
    out_path: str,
    color: str,
) -> bool:
    plt = _load_matplotlib()
    if plt is None:
        return False

    pts_sorted = _sort_points(points)
    labels = _labels_for_points(pts_sorted)
    values = []
    for pt in pts_sorted:
        val = pt.get(y_key)
        values.append(None if val is None else float(val))

    if not labels:
        return False

    xs = list(range(len(labels)))
    ys = [0.0 if v is None else v for v in values]

    plt.figure(figsize=(8, 5))
    plt.plot(xs, ys, marker="o", linewidth=2.0, markersize=5.5, color=color)
    plt.fill_between(xs, ys, [0] * len(ys), color=color, alpha=0.15)
    plt.xticks(xs, labels)
    plt.xlabel("Slowdown (%)")
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(axis="y", linestyle="--", alpha=0.6)

    y_max = max(ys) if ys else 0.0
    y_pad = max(1.0, y_max * 0.03)
    for x, val in zip(xs, values):
        if val is None:
            label = "n/a"
            y = y_pad
        else:
            label = f"{val:.1f}"
            y = val + y_pad
        plt.text(x, y, label, ha="center", va="bottom", fontsize=8)

    try:
        plt.tight_layout()
        plt.savefig(out_path, bbox_inches="tight")
        sys.stdout.write(f"Wrote plot to {out_path}\n")
        return True
    finally:
        plt.close("all")


def plot_throughput(points: List[Dict[str, Optional[float]]], out_path: str) -> bool:
    return _plot_series(
        points=points,
        y_key="throughput",
        y_label="Throughput (ops/s)",
        title="Cassandra Throughput vs Slowdown",
        out_path=out_path,
        color="#2a9d8f",
    )


def plot_latency(points: List[Dict[str, Optional[float]]], out_path: str) -> bool:
    return _plot_series(
        points=points,
        y_key="latency",
        y_label="Latency (microseconds)",
        title="Cassandra Latency vs Slowdown",
        out_path=out_path,
        color="#d97706",
    )


def plot_relative(points: List[Dict[str, Optional[float]]], out_path: str) -> bool:
    plt = _load_matplotlib()
    if plt is None:
        return False

    pts_sorted = _sort_points(points)
    labels = _labels_for_points(pts_sorted)
    thr_vals = []
    lat_vals = []
    for pt in pts_sorted:
        t = pt.get("throughput_slowdown_pct")
        l = pt.get("latency_increase_pct")
        thr_vals.append(0.0 if t is None else float(t))
        lat_vals.append(0.0 if l is None else float(l))

    if not labels:
        return False

    xs = list(range(len(labels)))
    width = 0.38
    plt.figure(figsize=(10, 5))
    bars_thr = plt.bar([x - width / 2 for x in xs], thr_vals, width=width, color="#2563eb", label="Throughput slowdown %")
    bars_lat = plt.bar([x + width / 2 for x in xs], lat_vals, width=width, color="#ea580c", label="Latency increase %")
    plt.xticks(xs, labels)
    plt.xlabel("Slowdown setting")
    plt.ylabel("Relative change (%)")
    plt.title("Cassandra Relative Slowdown (Throughput + Latency)")
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.legend(loc="upper left")

    for bars, vals in ((bars_thr, thr_vals), (bars_lat, lat_vals)):
        for bar, val in zip(bars, vals):
            y = val + max(1.0, val * 0.02)
            plt.text(bar.get_x() + bar.get_width() / 2, y, f"{val:.1f}%", ha="center", va="bottom", fontsize=8)

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
        help="Path to write normalized throughput plot (default: cass_normalized_throughput.png)",
    )
    parser.add_argument(
        "--throughput-plot-out",
        default="cass_throughput_vs_slowdown.png",
        help="Path to write raw throughput plot (default: cass_throughput_vs_slowdown.png)",
    )
    parser.add_argument(
        "--relative-plot-out",
        default="cass_relative_slowdown.png",
        help="Path to write relative throughput/latency slowdown bars (default: cass_relative_slowdown.png)",
    )
    parser.add_argument(
        "--latency-plot-out",
        default="cass_latency_vs_slowdown.png",
        help="Path to write raw latency plot (default: cass_latency_vs_slowdown.png)",
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

    if not args.no_plot:
        if args.plot_out:
            plot_normalized(points, args.plot_out)
        if args.relative_plot_out:
            plot_relative(points, args.relative_plot_out)
        if args.throughput_plot_out:
            plot_throughput(points, args.throughput_plot_out)
        if args.latency_plot_out:
            plot_latency(points, args.latency_plot_out)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
