#!/bin/bash

set -euo pipefail

BENCH_HOST="ccl4.cyber.lab"
BENCH_DIR_ABSOLUTE="/home/$USER/UNHBench"
RESULTS_DIR="/home/$USER/cass-results"
WORKLOAD_FILE="/tmp/cass-workload.in"
BENCH_SYSTEMS=("ccl1.cyber.lab" "ccl2.cyber.lab" "ccl3.cyber.lab")
TARGET_OPS="${TARGET_OPS:-0}"               # 0 = unlimited firehose
THREADCOUNT="${THREADCOUNT:-64}"            # push hard by default
CPU_DEGRADES="${CPU_DEGRADES:-0,25,50,75}"  # percent CPU removed from Cassandra containers
AGG_FILE="cassandra_cpu_degradation.csv"
PLOT_FILE="CPU_degradation.png"
MODE="${1:-both}"
MIN_OPS="${MIN_OPS:-100}"
WARMUP_WINDOWS="${WARMUP_WINDOWS:-2}"
CPU_SETTLE_SEC="${CPU_SETTLE_SEC:-5}"
SSH_USER="${SSH_USER:-$USER}"
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"}
CASSANDRA_CONTAINER_PREFIX="${CASSANDRA_CONTAINER_PREFIX:-cassandra-node}"
DOCKER_CMD="${DOCKER_CMD:-docker}"

case "$MODE" in
    test|visualize|both) ;;
    *)
        echo "Usage: $0 [test|visualize|both] (default: both)" >&2
        exit 1
        ;;
esac

# Optional overrides via CSV.
if [ -n "${BENCH_SYSTEMS_CSV:-}" ]; then
    IFS=',' read -r -a BENCH_SYSTEMS <<< "$BENCH_SYSTEMS_CSV"
fi

make_systems_value() {
    local IFS=,
    echo "${BENCH_SYSTEMS[*]}"
}

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

normalize_pct_value() {
    local raw="${1:-0}"
    raw="${raw//%/}"
    raw="${raw// /}"
    raw="${raw#+}"
    raw="${raw#-}"
    echo "${raw:-0}"
}

SYSTEMS_CSV="$(make_systems_value)"
IFS=',' read -r -a SYSTEMS <<< "$SYSTEMS_CSV"
if [ "${#SYSTEMS[@]}" -eq 0 ]; then
    echo "No Cassandra hosts provided" >&2
    exit 1
fi

IFS=',' read -r -a CPU_STEPS_RAW <<< "$CPU_DEGRADES"
CPU_STEPS=()
for pct in "${CPU_STEPS_RAW[@]}"; do
    CPU_STEPS+=("$(normalize_pct_value "$pct")")
done
[ "${#CPU_STEPS[@]}" -eq 0 ] && CPU_STEPS=(0 25 50 75)

if [ -n "${CASSANDRA_CONTAINERS_CSV:-}" ]; then
    IFS=',' read -r -a CONTAINERS <<< "$CASSANDRA_CONTAINERS_CSV"
else
    CONTAINERS=()
fi
if [ "${#CONTAINERS[@]}" -ne "${#SYSTEMS[@]}" ]; then
    CONTAINERS=()
    for idx in "${!SYSTEMS[@]}"; do
        CONTAINERS+=("${CASSANDRA_CONTAINER_PREFIX}-$((idx + 1))")
    done
fi

CORE_COUNTS=()

fetch_core_counts() {
    CORE_COUNTS=()
    for host in "${SYSTEMS[@]}"; do
        cores_raw=$(ssh ${SSH_OPTS} "$SSH_USER@$host" "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; if command -v nproc >/dev/null 2>&1; then nproc --all; fi; if command -v getconf >/dev/null 2>&1; then getconf _NPROCESSORS_ONLN; fi; if command -v $DOCKER_CMD >/dev/null 2>&1; then $DOCKER_CMD info --format '{{.NCPU}}'; fi; awk '/^processor/{c++} END{print c+0}' /proc/cpuinfo" 2>/dev/null || true)
        cores=$(printf '%s' "$cores_raw" | head -n1 | tr -dc '0-9')
        if [ -z "$cores" ] || ! [[ "$cores" =~ ^[0-9]+$ ]] || [ "$cores" -lt 1 ]; then
            echo "Unable to determine CPU count for $host (check SSH access and nproc/getconf availability)" >&2
            echo "Command output from $host: ${cores_raw:-<empty>}" >&2
            return 1
        fi
        CORE_COUNTS+=("$cores")
    done
    return 0
}

verify_containers() {
    for idx in "${!SYSTEMS[@]}"; do
        host="${SYSTEMS[$idx]}"
        container="${CONTAINERS[$idx]}"
        if ! ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD inspect $container >/dev/null 2>&1"; then
            echo "Container $container not found on $host" >&2
            return 1
        fi
    done
    return 0
}

apply_cpu_limit() {
    local percent_raw="$1"
    local percent
    percent=$(normalize_pct_value "$percent_raw")
    for idx in "${!SYSTEMS[@]}"; do
        host="${SYSTEMS[$idx]}"
        container="${CONTAINERS[$idx]}"
        cores="${CORE_COUNTS[$idx]:-}"
        available="unknown"
        if [[ "$percent" =~ ^[0-9]+$ ]]; then
            available=$((100 - percent))
        fi

        if [ -z "$container" ]; then
            echo "Missing container name for $host" >&2
            return 1
        fi

        if [ "$percent" = "0" ]; then
            log "Setting CPU to unlimited on $host ($container)"
            ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-quota=-1 --cpu-period=100000 \"$container\"" || return 1
            continue
        fi

        if [ -z "$cores" ]; then
            echo "Missing core count for $host" >&2
            return 1
        fi

        quota=$(awk -v cores="$cores" -v drop="$percent" 'BEGIN { allowed=cores*(100-drop)/100; if (allowed<0.1) allowed=0.1; printf("%.0f", allowed*100000); }')
        log "Applying ${percent}% CPU drop (limit=${available}% of cores) on $host ($container) -> quota ${quota}/100000"
        ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-period=100000 --cpu-quota=$quota \"$container\"" || return 1
    done
}

restore_cpu_limits() {
    for idx in "${!SYSTEMS[@]}"; do
        host="${SYSTEMS[$idx]}"
        container="${CONTAINERS[$idx]:-}"
        [ -z "$container" ] && continue
        ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-quota=-1 --cpu-period=100000 \"$container\"" >/dev/null 2>&1 || true
    done
}

run_remote_bench() {
    local pct="$1"
    local prefix="cassandra_cpu_${pct}"
    local run_dir="$RESULTS_DIR/cpu_${pct}"
    local output

    output=$(ssh "$USER@$BENCH_HOST" "BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' RESULTS_DIR='$RESULTS_DIR' WORKLOAD_FILE='$WORKLOAD_FILE' SYSTEMS_CSV='$SYSTEMS_CSV' THREADCOUNT='$THREADCOUNT' TARGET_OPS='$TARGET_OPS' MIN_OPS='$MIN_OPS' WARMUP_WINDOWS='$WARMUP_WINDOWS' RUN_DIR='$run_dir' PREFIX='$prefix' bash -s" <<'EOF'
set -euo pipefail

chmod -R u+rwX "$RESULTS_DIR" 2>/dev/null || true
if [ -d "$RUN_DIR" ]; then
    chmod -R u+rwX "$RUN_DIR" 2>/dev/null || true
    rm -rf "$RUN_DIR" 2>/dev/null || true
fi
mkdir -p "$RESULTS_DIR" "$RUN_DIR"
# Try to fix permissions if prior runs left root-owned files.
chmod u+rwX "$RESULTS_DIR" "$RUN_DIR" 2>/dev/null || true

cat <<EOWF > "$WORKLOAD_FILE"
workload=core
recordcount=10000
operationcount=100000000
readproportion=0.50
updateproportion=0.50
cassandra.cluster=$SYSTEMS_CSV
cassandra.hosts=$SYSTEMS_CSV
cassandra.port=9042
port=9042
cassandra.keyspace=ycsb
cassandra.table=usertable
cassandra.username=
cassandra.password=
cassandra.localdatacenter=dc1
cassandra.readconsistencylevel=ONE
cassandra.writeconsistencylevel=ONE
threadcount=$THREADCOUNT
target=$TARGET_OPS
loadmode=duration
loadduration=25
measurement.type=raw
csvfilename=$PREFIX
EOWF

LOG_FILE="$RUN_DIR/run.log"
THROUGHPUT=""
SUMMARY_RC=""
PREFIX_BASE=""
SUMMARY_SCRIPT="${SUMMARY_STATS_PATH:-}"

pushd "$RUN_DIR" >/dev/null
set +e
"$BENCH_DIR_ABSOLUTE/bin/unh-bench" run cassandra -P "$WORKLOAD_FILE" 2>&1 | tee "$LOG_FILE"
run_rc=${PIPESTATUS[0]}
set -e
popd >/dev/null

# Derive the actual prefix from emitted CSVs (handles driver-added suffixes like _primary_<ts>)
first_csv=$(ls "$RUN_DIR/${PREFIX}"*.csv 2>/dev/null | head -n1 || true)
if [ -n "$first_csv" ]; then
    PREFIX_BASE=$(python3 - "$first_csv" <<'PY'
import os, re, sys
path = sys.argv[1]
name = os.path.basename(path)
if name.endswith(".csv"):
    name = name[:-4]
# strip trailing _<digits> groups (timestamps, sequence ids)
name = re.sub(r'(_\d+)+$', '', name)
print(name)
PY
)
fi
[ -z "$PREFIX_BASE" ] && PREFIX_BASE="$PREFIX"

# Locate summary_stats.py on bench host if not provided
if [ -z "$SUMMARY_SCRIPT" ]; then
    SUMMARY_SCRIPT=$(find "$BENCH_DIR_ABSOLUTE" -name summary_stats.py -print -quit 2>/dev/null || true)
fi

if [ "$run_rc" -eq 0 ] || [ -n "$first_csv" ]; then
    # Try summary_stats first
    if [ -n "$SUMMARY_SCRIPT" ]; then
        if python3 "$SUMMARY_SCRIPT" -p "$PREFIX_BASE" -f "$RUN_DIR/" -w 1 >/dev/null 2>&1; then
            SUMMARY_RC=0
        else
            SUMMARY_RC=$?
        fi
    else
        SUMMARY_RC=127
    fi

    SUM_FILE="$RUN_DIR/${PREFIX_BASE}_1_sumstat.csv"
    SUM_FILE_PRESENT="no"
    if [ -f "$SUM_FILE" ]; then
        SUM_FILE_PRESENT="yes"
        THROUGHPUT=$(python3 - "$SUM_FILE" "$MIN_OPS" "$WARMUP_WINDOWS" <<'PY'
import csv, sys
path, min_ops, warmup = sys.argv[1:]
min_ops = float(min_ops)
warmup = int(warmup)
with open(path, newline="") as f:
    rows = list(csv.DictReader(f))
rows = rows[warmup:] if warmup > 0 else rows
ops = []
for row in rows:
    try:
        op = float(row.get("OpPS", 0))
    except (TypeError, ValueError):
        continue
    if op < min_ops:
        continue
    ops.append(op)
if ops:
    ops.sort()
    mid = len(ops)//2
    med_op = (ops[mid - 1] + ops[mid]) / 2 if len(ops) % 2 == 0 else ops[mid]
    print(med_op)
PY
)
    fi
fi

# Fallback: parse throughput from bench stdout log
if [ -z "${THROUGHPUT:-}" ]; then
    THROUGHPUT=$(python3 - "$LOG_FILE" <<'PY'
import re, sys
log_path = sys.argv[1]
try:
    data = open(log_path, "r", encoding="utf-8", errors="ignore").read().splitlines()
except OSError:
    sys.exit()
thr = ""
pat = re.compile(r"Throughput\\(ops/sec\\).*?([0-9]+(?:\\.[0-9]+)?)")
for line in reversed(data):
    m = pat.search(line)
    if m:
        thr = m.group(1)
        break
if thr:
    print(thr)
PY
)
fi

echo "RUN_RC=$run_rc"
echo "SUMMARY_RC=${SUMMARY_RC:-}"
echo "PREFIX_BASE=${PREFIX_BASE:-}"
echo "SUMMARY_SCRIPT=${SUMMARY_SCRIPT:-}"
echo "SUM_FILE=${SUM_FILE:-}"
echo "SUM_FILE_PRESENT=${SUM_FILE_PRESENT:-no}"
echo "THROUGHPUT=${THROUGHPUT:-}"
EOF
)

    printf '%s\n' "$output"
}

create_agg_on_bench() {
    local agg_data="$1"   # comma-separated pct:throughput entries
    local cpu_steps_str="$2" # space-separated normalized pct list
    ssh "$USER@$BENCH_HOST" "RESULTS_DIR='$RESULTS_DIR' AGG_FILE='$AGG_FILE' PLOT_FILE='$PLOT_FILE' AGG_DATA='$agg_data' CPU_STEPS_STR='$cpu_steps_str' BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' bash -s" <<'EOF'
set -euo pipefail

AGG_PATH="$RESULTS_DIR/$AGG_FILE"
COMBINED="$RESULTS_DIR/cassandra_combined_sumstat.csv"

mkdir -p "$RESULTS_DIR"
rm -f "$AGG_PATH" "$COMBINED"

python3 - "$AGG_PATH" "$AGG_DATA" <<'PY'
import csv, sys

out_path, data = sys.argv[1:]
pairs = [p for p in data.split(",") if p]
rows = []
for pair in pairs:
    if ":" not in pair:
        continue
    pct_raw, thr_raw = pair.split(":", 1)
    try:
        pct = float(pct_raw)
        thr = float(thr_raw)
    except ValueError:
        continue
    rows.append((pct, thr))

rows.sort(key=lambda r: r[0])
if not rows:
    sys.exit("No rows to write")

baseline = None
for pct, thr in rows:
    if pct == 0:
        baseline = thr
        break
if baseline is None:
    baseline = rows[0][1]

with open(out_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["CpuDegradationPct", "ThroughputOps", "NormalizedThroughput"])
    for pct, thr in rows:
        norm = thr / baseline if baseline > 0 else ""
        w.writerow([pct, thr, norm])
PY

IFS=' ' read -r -a CPU_STEPS <<< "$CPU_STEPS_STR"
SUM_FILES=()
for pct in "${CPU_STEPS[@]}"; do
    for path in "$RESULTS_DIR/cpu_${pct}/"*"_1_sumstat.csv"; do
        [ -f "$path" ] || continue
        SUM_FILES+=("$path")
        break
    done
done

if [ "${#SUM_FILES[@]}" -gt 0 ]; then
    python3 - "$COMBINED" "${SUM_FILES[@]}" <<'PY'
import csv
import sys

out = sys.argv[1]
inputs = sys.argv[2:]

header = None
rows = []
for path in inputs:
    with open(path, newline="") as f:
        r = csv.reader(f)
        try:
            h = next(r)
        except StopIteration:
            continue
        if header is None:
            header = h
        rows.extend(list(r))

with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(header if header else [])
    w.writerows(rows)
PY
fi

python3 - "$AGG_PATH" "$RESULTS_DIR" "$PLOT_FILE" <<'PY'
import csv
import os
import sys

agg_path, out_dir, out_name = sys.argv[1:]

pcts = []
norms = []
with open(agg_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            pct = float(row.get("CpuDegradationPct", 0))
            norm = float(row.get("NormalizedThroughput", 0))
        except (TypeError, ValueError):
            continue
        pcts.append(pct)
        norms.append(norm)

if not pcts:
    sys.exit("No data to plot from " + agg_path)

labels = [f"{pct:.0f}%" for pct in pcts]

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 5))
plt.bar(labels, norms, color="steelblue")
plt.xlabel("CPU slowdown (%)")
plt.ylabel("Normalized throughput (vs 100%)")
plt.title("Cassandra throughput vs CPU slowdown")
plt.ylim(0, max(norms) * 1.1 if norms else 1)
plt.grid(axis="y", linestyle="--", alpha=0.6)

outfile = os.path.join(out_dir, out_name)
plt.savefig(outfile, bbox_inches="tight")
print(f"Wrote plot to {outfile}")
PY
EOF
}

plot_existing_only() {
    ssh "$USER@$BENCH_HOST" "RESULTS_DIR='$RESULTS_DIR' AGG_FILE='$AGG_FILE' PLOT_FILE='$PLOT_FILE' bash -s" <<'EOF'
set -euo pipefail
AGG_PATH="$RESULTS_DIR/$AGG_FILE"
if [ ! -f "$AGG_PATH" ]; then
    echo "Aggregated file not found at $AGG_PATH" >&2
    exit 1
fi

python3 - "$AGG_PATH" "$RESULTS_DIR" "$PLOT_FILE" <<'PY'
import csv
import os
import sys

agg_path, out_dir, out_name = sys.argv[1:]

pcts = []
norms = []
with open(agg_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            pct = float(row.get("CpuDegradationPct", 0))
            norm = float(row.get("NormalizedThroughput", 0))
        except (TypeError, ValueError):
            continue
        pcts.append(pct)
        norms.append(norm)

if not pcts:
    sys.exit("No data to plot from " + agg_path)

labels = [f"{pct:.0f}%" for pct in pcts]

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 5))
plt.bar(labels, norms, color="steelblue")
plt.xlabel("CPU slowdown (%)")
plt.ylabel("Normalized throughput (vs 100%)")
plt.title("Cassandra throughput vs CPU slowdown")
plt.ylim(0, max(norms) * 1.1 if norms else 1)
plt.grid(axis="y", linestyle="--", alpha=0.6)

outfile = os.path.join(out_dir, out_name)
plt.savefig(outfile, bbox_inches="tight")
print(f"Wrote plot to {outfile}")
PY
EOF
}

trap restore_cpu_limits EXIT

if [[ "$MODE" == "visualize" ]]; then
    plot_existing_only
    exit 0
fi

log "Starting Cassandra Benchmark on $BENCH_HOST $SYSTEMS_CSV"

fetch_core_counts
verify_containers

AGG_ROWS=()

for pct in "${CPU_STEPS[@]}"; do
    limit_display="unknown"
    if [[ "$pct" =~ ^[0-9]+$ ]]; then
        limit_display=$((100 - pct))
    fi
    apply_cpu_limit "$pct"
    if [ "${CPU_SETTLE_SEC:-0}" -gt 0 ]; then
        log "Waiting ${CPU_SETTLE_SEC}s after CPU change for Cassandra to settle"
        sleep "$CPU_SETTLE_SEC"
    fi
    log "Running Cassandra load at CPU limit ${limit_display}% (slowdown=${pct}%, target=$TARGET_OPS, threads=$THREADCOUNT)"
    output="$(run_remote_bench "$pct")"
    run_rc=$(printf '%s\n' "$output" | awk -F= '/^RUN_RC=/{print $2; exit}')
    summary_rc=$(printf '%s\n' "$output" | awk -F= '/^SUMMARY_RC=/{print $2; exit}')
    prefix_base=$(printf '%s\n' "$output" | awk -F= '/^PREFIX_BASE=/{print $2; exit}')
    throughput=$(printf '%s\n' "$output" | awk -F= '/^THROUGHPUT=/{print $2; exit}')
    sum_file=$(printf '%s\n' "$output" | awk -F= '/^SUM_FILE=/{print $2; exit}')
    sum_present=$(printf '%s\n' "$output" | awk -F= '/^SUM_FILE_PRESENT=/{print $2; exit}')
    if [ -n "${run_rc:-}" ] && [ "$run_rc" != "0" ]; then
        log "Bench run exited with code $run_rc at CPU limit ${limit_display}% (slowdown=${pct}%)"
    fi
    if [ -n "${summary_rc:-}" ] && [ "$summary_rc" != "0" ]; then
        log "summary_stats.py failed (code $summary_rc) for prefix '${prefix_base:-}'"
    fi
    if [ "${sum_present:-no}" = "yes" ]; then
        log "Summary CSV captured at ${sum_file:-<unknown>} for slowdown ${pct}%"
    else
        log "No sumstat CSV found for slowdown ${pct}%"
    fi
    if [ -z "${throughput:-}" ]; then
        log "No throughput captured for slowdown ${pct}%"
    else
        log "Captured throughput=${throughput} ops/s at CPU limit ${limit_display}% (slowdown=${pct}%)"
        AGG_ROWS+=("${pct}:${throughput}")
    fi
done

restore_cpu_limits

if [ "${#AGG_ROWS[@]}" -eq 0 ]; then
    echo "No summary data collected; aborting." >&2
    exit 1
fi

agg_data=$(IFS=','; echo "${AGG_ROWS[*]}")
cpu_steps_str=$(IFS=' '; echo "${CPU_STEPS[*]}")
create_agg_on_bench "$agg_data" "$cpu_steps_str"
