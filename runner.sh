#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CASS_CLUSTER_SCRIPT="${CASS_CLUSTER_SCRIPT:-${SCRIPT_DIR}/cass-cluster.sh}"
CASS_MODE_SCRIPT="${CASS_MODE_SCRIPT:-${SCRIPT_DIR}/cass.sh}"
MODE_LINES_PLOT_SCRIPT="${MODE_LINES_PLOT_SCRIPT:-${SCRIPT_DIR}/plot_mode_lines.py}"

OUT_BASE="${OUT_BASE:-${SCRIPT_DIR}/out}"
LOG_DIR="${LOG_DIR:-${OUT_BASE}/logs}"

LATENCY_TARGETS_MS=(${LATENCY_TARGETS_MS:-25})
MODES=(${MODES:-cpu network_bandwidth network_latency})

CPU_STEPS=(${CPU_STEPS:-4 3 2 1})
MEMORY_STEPS=(${MEMORY_STEPS:-65536 49152 32768 16384 8192 4096})
NETWORK_BANDWIDTH_STEPS=(${NETWORK_BANDWIDTH_STEPS:-500 100 50 25 10})
NETWORK_LATENCY_STEPS=(${NETWORK_LATENCY_STEPS:-0.25ms 0.5ms 1ms 5ms})

MAX_TRIAL_ATTEMPTS="${MAX_TRIAL_ATTEMPTS:-3}"
FIND_TRIALS="${FIND_TRIALS:-3}"
VALIDATION_TRIALS="${VALIDATION_TRIALS:-5}"

MIN_THROUGHPUT="${MIN_THROUGHPUT:-1}"
INITIAL_THROUGHPUT="${INITIAL_THROUGHPUT:-32000}"
MAX_THROUGHPUT="${MAX_THROUGHPUT:-512000}"
THROUGHPUT_GRANULARITY="${THROUGHPUT_GRANULARITY:-500}"
MAX_EVAL_POINTS="${MAX_EVAL_POINTS:-14}"

SNAPSHOT_NAME="${SNAPSHOT_NAME:-baseline}"
CLEAN_OUT="${CLEAN_OUT:-false}"
REMOVE_SNAPSHOTS_ON_DOWN="${REMOVE_SNAPSHOTS_ON_DOWN:-true}"

# Bench run profile
CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"
SILENCE="${SILENCE:-true}"
THREAD_COUNT="${THREAD_COUNT:-256}"
FIND_RUN_DURATION="${FIND_RUN_DURATION:-30}"
VALIDATION_RUN_DURATION="${VALIDATION_RUN_DURATION:-90}"
RECORD_COUNT="${RECORD_COUNT:-200000}"
OPERATION_COUNT="${OPERATION_COUNT:-2000000}"
CASS_CONNECTIONS="${CASS_CONNECTIONS:-8}"
RESTORE_BEFORE_STEP="${RESTORE_BEFORE_STEP:-true}"

# Slowdown defaults
SLOWDOWN_NODES_CSV="${SLOWDOWN_NODES_CSV:-ccl1.cyber.lab}"
DEFAULT_CORES="${DEFAULT_CORES:-4}"
DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT:-10000}"
DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS:-0ms}"
DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB:-65536}"

CLUSTER_READY="false"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

cleanup() {
    local rc=$?
    if [[ "$CLUSTER_READY" == "true" ]]; then
        log "Teardown: destroying Cassandra cluster and snapshots"
        if ! REMOVE_SNAPSHOTS_ON_DOWN="$REMOVE_SNAPSHOTS_ON_DOWN" bash "$CASS_CLUSTER_SCRIPT" down; then
            log "WARNING: cluster teardown returned non-zero"
        fi
    fi
    return "$rc"
}
trap cleanup EXIT

mode_steps() {
    local mode="$1"
    case "$mode" in
        cpu) printf '%s\n' "${CPU_STEPS[@]}" ;;
        memory) printf '%s\n' "${MEMORY_STEPS[@]}" ;;
        network_bandwidth) printf '%s\n' "${NETWORK_BANDWIDTH_STEPS[@]}" ;;
        network_latency) printf '%s\n' "${NETWORK_LATENCY_STEPS[@]}" ;;
        *) return 1 ;;
    esac
}

if [[ "$CLEAN_OUT" == "true" ]]; then
    rm -rf "$OUT_BASE"
fi
mkdir -p "$OUT_BASE" "$LOG_DIR"

log "Bringing Cassandra cluster up once"
bash "$CASS_CLUSTER_SCRIPT" up

log "Creating baseline snapshot '${SNAPSHOT_NAME}'"
bash "$CASS_CLUSTER_SCRIPT" snapshot-create "$SNAPSHOT_NAME"

CLUSTER_READY="true"

failures=()

for latency_target in "${LATENCY_TARGETS_MS[@]}"; do
    target_dir="${OUT_BASE}/max_latency_target_${latency_target}"
    mkdir -p "$target_dir"

    log "==== LATENCY TARGET ${latency_target}ms -> ${target_dir} ===="

    for mode in "${MODES[@]}"; do
        mapfile -t step_args < <(mode_steps "$mode")
        if [[ "${#step_args[@]}" -eq 0 ]]; then
            log "Skipping mode ${mode}: no steps"
            continue
        fi

        run_ts="$(date +%Y%m%d_%H%M%S)"
        log_file="${LOG_DIR}/target_${latency_target}_${mode}_${run_ts}.log"

        log "Running mode=${mode} with ${#step_args[@]} step(s) at latency target ${latency_target}ms"

        set +e
        OUT_DIR="$target_dir" \
            LATENCY_TARGET_MS="$latency_target" \
            SLOWDOWN_TYPE="$mode" \
            SNAPSHOT_NAME="$SNAPSHOT_NAME" \
            RESTORE_BEFORE_STEP="$RESTORE_BEFORE_STEP" \
            MAX_TRIAL_ATTEMPTS="$MAX_TRIAL_ATTEMPTS" \
            MIN_THROUGHPUT="$MIN_THROUGHPUT" \
            INITIAL_THROUGHPUT="$INITIAL_THROUGHPUT" \
            MAX_THROUGHPUT="$MAX_THROUGHPUT" \
            THROUGHPUT_GRANULARITY="$THROUGHPUT_GRANULARITY" \
            MAX_EVAL_POINTS="$MAX_EVAL_POINTS" \
            FIND_TRIALS="$FIND_TRIALS" \
            VALIDATION_TRIALS="$VALIDATION_TRIALS" \
            CLIENT_TYPE="$CLIENT_TYPE" \
            SILENCE="$SILENCE" \
            THREAD_COUNT="$THREAD_COUNT" \
            FIND_RUN_DURATION="$FIND_RUN_DURATION" \
            VALIDATION_RUN_DURATION="$VALIDATION_RUN_DURATION" \
            RECORD_COUNT="$RECORD_COUNT" \
            OPERATION_COUNT="$OPERATION_COUNT" \
            CASS_CONNECTIONS="$CASS_CONNECTIONS" \
            SLOWDOWN_NODES_CSV="$SLOWDOWN_NODES_CSV" \
            DEFAULT_CORES="$DEFAULT_CORES" \
            DEFAULT_BANDWIDTH_MBIT="$DEFAULT_BANDWIDTH_MBIT" \
            DEFAULT_LATENCY_MS="$DEFAULT_LATENCY_MS" \
            DEFAULT_MEMORY_MB="$DEFAULT_MEMORY_MB" \
            bash "$CASS_MODE_SCRIPT" "${step_args[@]}" 2>&1 | tee "$log_file"
        rc=${PIPESTATUS[0]}
        set -e

        if [[ "$rc" -ne 0 ]]; then
            failures+=("latency_target=${latency_target},mode=${mode},log=${log_file}")
            log "FAILED mode=${mode} latency_target=${latency_target}"
            continue
        fi

        log "DONE mode=${mode} latency_target=${latency_target}"
    done

done

for mode in "${MODES[@]}"; do
    merged_csv="${OUT_BASE}/${mode}_throughput_vs_degradation.csv"
    merged_png="${OUT_BASE}/${mode}_throughput_vs_degradation.png"
    if ! python3 "$MODE_LINES_PLOT_SCRIPT" \
        --out-base "$OUT_BASE" \
        --mode "$mode" \
        --output-csv "$merged_csv" \
        --output-png "$merged_png" >/dev/null 2>&1; then
        log "WARNING: Could not build throughput-vs-degradation plot for mode=${mode}"
    else
        log "Built throughput-vs-degradation plot for mode=${mode}: ${merged_png}"
    fi
done

if [[ "${#failures[@]}" -gt 0 ]]; then
    log "Run completed with failures:"
    printf '%s\n' "${failures[@]}"
    exit 1
fi

log "Run completed successfully"
