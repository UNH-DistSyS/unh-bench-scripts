#!/bin/bash

set -euo pipefail

# Keep this lean: run Cassandra under different CPU caps, execute the bench remotely,
# and generate summary CSVs with summary_stats.py. No plotting or extra rendering.

BENCH_HOST="ccl4.cyber.lab"
BENCH_DIR_ABSOLUTE="/home/$USER/UNHBench"
RESULTS_DIR="/home/$USER/cass-results"
WORKLOAD_FILE="/tmp/cass-workload.in"
BENCH_SYSTEMS=("ccl1.cyber.lab" "ccl2.cyber.lab" "ccl3.cyber.lab")
TARGET_OPS="${TARGET_OPS:-0}"
THREADCOUNT="${THREADCOUNT:-256}"
CPU_DEGRADES="${CPU_DEGRADES:-0,25,50,75}"
WARMUP_WINDOWS="${WARMUP_WINDOWS:-2}"
MIN_OPS="${MIN_OPS:-100}"
CPU_SETTLE_SEC="${CPU_SETTLE_SEC:-5}"
SSH_USER="${SSH_USER:-$USER}"
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"}
CASSANDRA_CONTAINER_PREFIX="${CASSANDRA_CONTAINER_PREFIX:-cassandra-node}"
SUMMARY_SCRIPT="${SUMMARY_SCRIPT:-$BENCH_DIR_ABSOLUTE/tool/analysis/summary_stats.py}"
DOCKER_CMD="${DOCKER_CMD:-docker}"
LOCAL_VALUES_SCRIPT="${LOCAL_VALUES_SCRIPT:-./process_cass_values.py}"
RECORDCOUNT="${RECORDCOUNT:-1000000}"
LOADDURATION="${LOADDURATION:-120}"
FIELD_LENGTH="${FIELD_LENGTH:-1024}"

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

make_systems_value() {
    local IFS=,
    echo "${BENCH_SYSTEMS[*]}"
}

# Optional overrides via CSV env var.
if [ -n "${BENCH_SYSTEMS_CSV:-}" ]; then
    IFS=',' read -r -a BENCH_SYSTEMS <<< "$BENCH_SYSTEMS_CSV"
fi

SYSTEMS_CSV="$(make_systems_value)"
IFS=',' read -r -a SYSTEMS <<< "$SYSTEMS_CSV"
if [ "${#SYSTEMS[@]}" -eq 0 ]; then
    echo "No Cassandra hosts provided" >&2
    exit 1
fi

# Build container names or accept explicit list.
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

# Parse CPU steps.
IFS=',' read -r -a CPU_STEPS_RAW <<< "$CPU_DEGRADES"
CPU_STEPS=()
for pct in "${CPU_STEPS_RAW[@]}"; do
    CPU_STEPS+=("$(normalize_pct_value "$pct")")
done
[ "${#CPU_STEPS[@]}" -eq 0 ] && CPU_STEPS=(0 25 50 75)

CORE_COUNTS=()
VALUES=()

fetch_core_counts() {
    CORE_COUNTS=()
    for host in "${SYSTEMS[@]}"; do
        cores=$(ssh ${SSH_OPTS} "$SSH_USER@$host" "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; (nproc --all || getconf _NPROCESSORS_ONLN) 2>/dev/null" | head -n1 | tr -dc '0-9')
        if [ -z "$cores" ] || ! [[ "$cores" =~ ^[0-9]+$ ]] || [ "$cores" -lt 1 ]; then
            echo "Unable to determine CPU count for $host" >&2
            return 1
        fi
        CORE_COUNTS+=("$cores")
    done
}

apply_cpu_limit() {
    local percent_raw="$1"
    local percent
    percent=$(normalize_pct_value "$percent_raw")
    for idx in "${!SYSTEMS[@]}"; do
        host="${SYSTEMS[$idx]}"
        container="${CONTAINERS[$idx]}"
        cores="${CORE_COUNTS[$idx]}"

        if [ "$percent" = "0" ]; then
            log "Setting CPU to unlimited on $host ($container)"
            ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-quota=-1 --cpu-period=100000 \"$container\"" >/dev/null
            continue
        fi

        quota=$(awk -v cores="$cores" -v drop="$percent" 'BEGIN { allowed=cores*(100-drop)/100; if (allowed<0.1) allowed=0.1; printf("%.0f", allowed*100000); }')
        log "Applying ${percent}% CPU drop on $host ($container) -> quota ${quota}/100000"
        ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-period=100000 --cpu-quota=$quota \"$container\"" >/dev/null
    done
}

restore_cpu_limits() {
    for idx in "${!SYSTEMS[@]}"; do
        host="${SYSTEMS[$idx]}"
        container="${CONTAINERS[$idx]}"
        ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD update --cpu-quota=-1 --cpu-period=100000 \"$container\"" >/dev/null 2>&1 || true
    done
}

run_bench_for_pct() {
    local pct="$1"
    local prefix="cassandra_cpu_${pct}"
    local run_dir="$RESULTS_DIR/cpu_${pct}"

    ssh ${SSH_OPTS} "$SSH_USER@$BENCH_HOST" "BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' RESULTS_DIR='$RESULTS_DIR' WORKLOAD_FILE='$WORKLOAD_FILE' THREADCOUNT='$THREADCOUNT' TARGET_OPS='$TARGET_OPS' SYSTEMS_CSV='$SYSTEMS_CSV' PREFIX='$prefix' RUN_DIR='$run_dir' SUMMARY_SCRIPT='$SUMMARY_SCRIPT' WARMUP_WINDOWS='$WARMUP_WINDOWS' MIN_OPS='$MIN_OPS' RECORDCOUNT='$RECORDCOUNT' LOADDURATION='$LOADDURATION' FIELD_LENGTH='$FIELD_LENGTH' bash -s" <<'EOF'
set -euo pipefail

rm -rf "$RUN_DIR" 2>/dev/null || true
mkdir -p "$RUN_DIR"

cat > "$WORKLOAD_FILE" <<EOW
workload=core
recordcount=$RECORDCOUNT
operationcount=100000000
readproportion=0.50
updateproportion=0.50
fieldlength=$FIELD_LENGTH
cassandra.cluster=$SYSTEMS_CSV
cassandra.hosts=$SYSTEMS_CSV
cassandra.port=9042
port=9042
cassandra.keyspace=ycsb
cassandra.table=usertable
cassandra.localdatacenter=dc1
cassandra.readconsistencylevel=ONE
cassandra.writeconsistencylevel=ONE
threadcount=$THREADCOUNT
target=$TARGET_OPS
loadmode=duration
loadduration=$LOADDURATION
measurement.type=raw
csvfilename=$PREFIX
EOW

pushd "$RUN_DIR" >/dev/null
"$BENCH_DIR_ABSOLUTE/bin/unh-bench" run cassandra -P "$WORKLOAD_FILE"
popd >/dev/null

EOF
}

trap restore_cpu_limits EXIT

log "Starting Cassandra bench on $BENCH_HOST against $SYSTEMS_CSV"
fetch_core_counts

agg_results() {
    #./cass-results/cpu_X/*.csv should be processed
    ssh ${SSH_OPTS} "$SSH_USER@$BENCH_HOST" "BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' RESULTS_DIR='$RESULTS_DIR' CPU_STEPS='${CPU_STEPS[*]}' SUMMARY_SCRIPT='$SUMMARY_SCRIPT' bash -s" <<'EOF'
set -euo pipefail
IFS=' ' read -r -a CPU_STEPS_ARR <<< "${CPU_STEPS:-}"
pushd "$RESULTS_DIR" >/dev/null
for pct in "${CPU_STEPS_ARR[@]}"; do
    # Log out the args for debug
    echo "Aggregating for CPU slowdown ${pct}%"
    python3 "$BENCH_DIR_ABSOLUTE/tool/analysis/summary_stats.py" -p "cassandra_cpu_${pct}" -f "$RESULTS_DIR/cpu_${pct}/" -w 1
    mv "${RESULTS_DIR}/cassandra_cpu_${pct}_1_sumstat.csv" "${RESULTS_DIR}/cassandra_cpu_${pct}_summary.csv"
done

popd >/dev/null
EOF
}

proc_values() {
    local raw
    raw=$(ssh ${SSH_OPTS} "$SSH_USER@$BENCH_HOST" "RESULTS_DIR='$RESULTS_DIR' CPU_STEPS='${CPU_STEPS[*]}' bash -s" <<'EOF'
set -euo pipefail
IFS=' ' read -r -a CPU_STEPS_ARR <<< "${CPU_STEPS:-}"

for pct in "${CPU_STEPS_ARR[@]}"; do
    file="${RESULTS_DIR}/cassandra_cpu_${pct}_summary.csv"
    if [ ! -f "$file" ]; then
        echo "${pct},N/A,N/A"
        continue
    fi

    python3 - "$file" "$pct" <<'PY'
import csv
import math
import sys

path, pct = sys.argv[1], sys.argv[2]

def mean(vals):
    return sum(vals) / len(vals) if vals else None

def fmt(val):
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return "N/A"
    return f"{val:.2f}"

throughputs = []
latencies = []

try:
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for t_key in ("Throughput", "OpPS"):
                v = row.get(t_key)
                if v is None:
                    continue
                try:
                    throughputs.append(float(v))
                    break
                except (TypeError, ValueError):
                    continue
            for l_key in ("Mean", "AverageLatency", "AvgLatency", "Latency"):
                v = row.get(l_key)
                if v is None:
                    continue
                try:
                    latencies.append(float(v))
                    break
                except (TypeError, ValueError):
                    continue
except OSError as exc:
    sys.stderr.write(f"Failed to read {path}: {exc}\n")

thr = mean(throughputs)
lat = mean(latencies)
print(f"{pct},{fmt(thr)},{fmt(lat)}")
PY
done
EOF
) || return

    VALUES=()
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        VALUES+=("$line")
    done <<< "$raw"
}

run_local_values_script() {
    if [ "${#VALUES[@]}" -eq 0 ]; then
        log "No VALUES collected; skipping local processor"
        return
    fi

    if [ -z "${LOCAL_VALUES_SCRIPT:-}" ]; then
        log "LOCAL_VALUES_SCRIPT not set; skipping local processor"
        return
    fi

    if [ ! -f "$LOCAL_VALUES_SCRIPT" ]; then
        log "Local values script not found at $LOCAL_VALUES_SCRIPT; skipping"
        return
    fi

    log "Running local values processor on ${#VALUES[@]} points via $LOCAL_VALUES_SCRIPT"
    if ! python3 "$LOCAL_VALUES_SCRIPT" "${VALUES[@]}"; then
        log "Local values processor failed"
    fi
}

for pct in "${CPU_STEPS[@]}"; do
    apply_cpu_limit "$pct"
    if [ "${CPU_SETTLE_SEC:-0}" -gt 0 ]; then
        log "Sleeping ${CPU_SETTLE_SEC}s for Cassandra to settle"
        sleep "$CPU_SETTLE_SEC"
    fi

    log "Running bench at CPU slowdown ${pct}% (threads=$THREADCOUNT target=$TARGET_OPS)"
    run_bench_for_pct "$pct"    
done

restore_cpu_limits

log "Aggregating results"
agg_results

log "Processing final throughput/latency values"
proc_values
if [ "${#VALUES[@]}" -gt 0 ]; then
    log "Collected data points (cpu%,avg_throughput,avg_latency): ${VALUES[*]}"
fi

run_local_values_script
