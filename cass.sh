#!/usr/bin/env bash
# cass.sh
# Run throughput search per slowdown step for a single latency target.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CASS_CLUSTER_SCRIPT="${CASS_CLUSTER_SCRIPT:-${SCRIPT_DIR}/cass-cluster.sh}"
CASS_EXPERIMENT_SCRIPT="${CASS_EXPERIMENT_SCRIPT:-${SCRIPT_DIR}/cass-experiment.sh}"

SLOWDOWN_TYPE_RAW="${SLOWDOWN_TYPE:-cpu}"
LATENCY_TARGET_MS="${LATENCY_TARGET_MS:-25}"

MIN_THROUGHPUT="${MIN_THROUGHPUT:-1}"
INITIAL_THROUGHPUT="${INITIAL_THROUGHPUT:-32000}"
MAX_THROUGHPUT="${MAX_THROUGHPUT:-512000}"
THROUGHPUT_GRANULARITY="${THROUGHPUT_GRANULARITY:-500}"
MAX_EVAL_POINTS="${MAX_EVAL_POINTS:-14}"

FIND_TRIALS="${FIND_TRIALS:-3}"
VALIDATION_TRIALS="${VALIDATION_TRIALS:-5}"
MAX_TRIAL_ATTEMPTS="${MAX_TRIAL_ATTEMPTS:-3}"
FIND_RUN_DURATION="${FIND_RUN_DURATION:-30}"
VALIDATION_RUN_DURATION="${VALIDATION_RUN_DURATION:-90}"

SNAPSHOT_NAME="${SNAPSHOT_NAME:-baseline}"
RESTORE_BEFORE_STEP="${RESTORE_BEFORE_STEP:-true}"
OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/out/max_latency_target_${LATENCY_TARGET_MS}}"

SLOWDOWN_NODES_CSV="${SLOWDOWN_NODES_CSV:-ccl1.cyber.lab}"
DEFAULT_CORES="${DEFAULT_CORES:-8}"
DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT:-10000}"
DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS:-0ms}"
DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB:-65536}"

CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"
SILENCE="${SILENCE:-true}"
THREAD_COUNT="${THREAD_COUNT:-512}"
RECORD_COUNT="${RECORD_COUNT:-200000}"
OPERATION_COUNT="${OPERATION_COUNT:-2000000}"
CASS_CONNECTIONS="${CASS_CONNECTIONS:-8}"
CLEAN_REMOTE_RESULTS="${CLEAN_REMOTE_RESULTS:-true}"

CPU_STEPS_DEFAULT=(4 3 2 1)
MEMORY_STEPS_DEFAULT=(65536 49152 32768 16384 8192 4096)
NETWORK_BANDWIDTH_STEPS_DEFAULT=(500 100 50 25 10)
NETWORK_LATENCY_STEPS_DEFAULT=(0.25ms 0.5ms 1ms 5ms)

MODE_SUMMARY_CSV=""
SLOWDOWN_TYPE=""
EXPERIMENT_SLOWDOWN_TYPE=""

declare -a FAILED_STEPS=()

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

die() {
    log "ERROR: $*"
    exit 1
}

append_log() {
    local log_file="$1"
    local msg="$2"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $msg" | tee -a "$log_file" >&2
}

is_positive_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

normalize_slowdown_type() {
    case "$1" in
        cpu|memory|network_bandwidth|network_latency) echo "$1" ;;
        network) echo "network_bandwidth" ;;
        latency) echo "network_latency" ;;
        *) return 1 ;;
    esac
}

experiment_slowdown_type() {
    case "$SLOWDOWN_TYPE" in
        cpu) echo "cpu" ;;
        memory) echo "memory" ;;
        network_bandwidth) echo "network" ;;
        network_latency) echo "latency" ;;
        *) die "Unsupported slowdown type: $SLOWDOWN_TYPE" ;;
    esac
}

default_steps_for_mode() {
    case "$SLOWDOWN_TYPE" in
        cpu) printf '%s\n' "${CPU_STEPS_DEFAULT[@]}" ;;
        memory) printf '%s\n' "${MEMORY_STEPS_DEFAULT[@]}" ;;
        network_bandwidth) printf '%s\n' "${NETWORK_BANDWIDTH_STEPS_DEFAULT[@]}" ;;
        network_latency) printf '%s\n' "${NETWORK_LATENCY_STEPS_DEFAULT[@]}" ;;
        *) die "Unsupported slowdown type: $SLOWDOWN_TYPE" ;;
    esac
}

format_step_label() {
    local step="$1"
    case "$SLOWDOWN_TYPE" in
        cpu)
            echo "${step}c"
            ;;
        memory)
            if [[ "$step" =~ ^[0-9]+$ ]] && (( step % 1024 == 0 )); then
                echo "$((step / 1024))GB"
            else
                echo "${step}MB"
            fi
            ;;
        network_bandwidth)
            if [[ "$step" =~ ^[0-9]+$ ]] && (( step >= 1000 )) && (( step % 1000 == 0 )); then
                echo "$((step / 1000))Gbps"
            else
                echo "${step}Mbps"
            fi
            ;;
        network_latency)
            echo "${step}"
            ;;
        *)
            echo "${step}"
            ;;
    esac
}

write_step_result() {
    local step_dir="$1"
    local step="$2"
    local step_label="$3"
    local max_target="$4"
    local observed_thr="$5"
    local observed_lat_us="$6"
    local validated="$7"

    printf "slowdown_type,step_value,step_label,latency_target_ms,max_target_throughput,observed_throughput_at_max,observed_latency_us_at_max,validated\n" > "${step_dir}/max_throughput_result.csv"
    printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$SLOWDOWN_TYPE" "$step" "$step_label" "$LATENCY_TARGET_MS" "$max_target" "$observed_thr" "$observed_lat_us" "$validated" >> "${step_dir}/max_throughput_result.csv"

    printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$SLOWDOWN_TYPE" "$step" "$step_label" "$LATENCY_TARGET_MS" "$max_target" "$observed_thr" "$observed_lat_us" "$validated" >> "$MODE_SUMMARY_CSV"
}

run_step() {
    local step="$1"
    local step_label
    local step_dir
    local run_log
    local max_line
    local max_target="0"
    local observed_thr="0"
    local observed_lat_us="999999999"
    local validated="false"
    local rc

    step_label="$(format_step_label "$step")"
    step_dir="${OUT_DIR}/${SLOWDOWN_TYPE}_${step_label}"
    run_log="${step_dir}/run.log"

    mkdir -p "$step_dir"

    append_log "$run_log" "Starting step ${step_label} for latency target ${LATENCY_TARGET_MS}ms"

    if [[ "$RESTORE_BEFORE_STEP" == "true" ]]; then
        append_log "$run_log" "Restoring snapshot '${SNAPSHOT_NAME}' before step ${step_label}"
        if ! bash "$CASS_CLUSTER_SCRIPT" snapshot-restore "$SNAPSHOT_NAME" >>"$run_log" 2>&1; then
            append_log "$run_log" "snapshot-restore failed for step ${step_label}"
            write_step_result "$step_dir" "$step" "$step_label" "$max_target" "$observed_thr" "$observed_lat_us" "$validated"
            FAILED_STEPS+=("${step_label}:snapshot-restore")
            return 1
        fi
    fi

    set +e
    SLOWDOWN_TYPE="$EXPERIMENT_SLOWDOWN_TYPE" \
        SLOWDOWN_VALUE="$step" \
        SLOWDOWN_NODES_CSV="$SLOWDOWN_NODES_CSV" \
        DEFAULT_CORES="$DEFAULT_CORES" \
        DEFAULT_BANDWIDTH_MBIT="$DEFAULT_BANDWIDTH_MBIT" \
        DEFAULT_LATENCY_MS="$DEFAULT_LATENCY_MS" \
        DEFAULT_MEMORY_MB="$DEFAULT_MEMORY_MB" \
        LATENCY_TARGET_MS="$LATENCY_TARGET_MS" \
        THROUGHPUT_MODE="search" \
        MIN_THROUGHPUT="$MIN_THROUGHPUT" \
        INITIAL_THROUGHPUT="$INITIAL_THROUGHPUT" \
        MAX_THROUGHPUT="$MAX_THROUGHPUT" \
        THROUGHPUT_GRANULARITY="$THROUGHPUT_GRANULARITY" \
        MAX_EVAL_POINTS="$MAX_EVAL_POINTS" \
        FIND_TRIALS="$FIND_TRIALS" \
        VALIDATION_TRIALS="$VALIDATION_TRIALS" \
        MAX_TRIAL_ATTEMPTS="$MAX_TRIAL_ATTEMPTS" \
        FIND_RUN_DURATION="$FIND_RUN_DURATION" \
        VALIDATION_RUN_DURATION="$VALIDATION_RUN_DURATION" \
        THREAD_COUNT="$THREAD_COUNT" \
        RECORD_COUNT="$RECORD_COUNT" \
        OPERATION_COUNT="$OPERATION_COUNT" \
        CASS_CONNECTIONS="$CASS_CONNECTIONS" \
        CLIENT_TYPE="$CLIENT_TYPE" \
        SILENCE="$SILENCE" \
        CLEAN_REMOTE_RESULTS="$CLEAN_REMOTE_RESULTS" \
        RUN_TAG="${SLOWDOWN_TYPE}_${step_label}_lat${LATENCY_TARGET_MS}_$(date +%s)" \
        bash "$CASS_EXPERIMENT_SCRIPT" 2>&1 | tee -a "$run_log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ "$rc" -ne 0 ]]; then
        append_log "$run_log" "Experiment failed for step ${step_label}"
        write_step_result "$step_dir" "$step" "$step_label" "$max_target" "$observed_thr" "$observed_lat_us" "$validated"
        FAILED_STEPS+=("${step_label}:experiment")
        return 1
    fi

    max_line="$(grep '^MAX_RESULT:' "$run_log" | tail -n1 || true)"
    if [[ -n "$max_line" ]]; then
        IFS=',' read -r max_target observed_thr observed_lat_us validated <<< "${max_line#MAX_RESULT:}"
    fi

    if [[ -z "$max_line" || -z "${max_target:-}" || -z "${observed_thr:-}" || -z "${observed_lat_us:-}" || -z "${validated:-}" ]]; then
        append_log "$run_log" "Missing or malformed MAX_RESULT for step ${step_label}"
        max_target="0"
        observed_thr="0"
        observed_lat_us="999999999"
        validated="false"
        FAILED_STEPS+=("${step_label}:parse")
        write_step_result "$step_dir" "$step" "$step_label" "$max_target" "$observed_thr" "$observed_lat_us" "$validated"
        return 1
    fi

    append_log "$run_log" "Completed step ${step_label}: max_target=${max_target}, observed_thr=${observed_thr}, observed_lat_us=${observed_lat_us}, validated=${validated}"
    write_step_result "$step_dir" "$step" "$step_label" "$max_target" "$observed_thr" "$observed_lat_us" "$validated"
    return 0
}

main() {
    local step

    SLOWDOWN_TYPE="$(normalize_slowdown_type "${SLOWDOWN_TYPE_RAW}")" || die "Unsupported SLOWDOWN_TYPE='${SLOWDOWN_TYPE_RAW}'"
    EXPERIMENT_SLOWDOWN_TYPE="$(experiment_slowdown_type)"

    for v in "$MIN_THROUGHPUT" "$INITIAL_THROUGHPUT" "$MAX_THROUGHPUT" "$THROUGHPUT_GRANULARITY" "$MAX_EVAL_POINTS" "$FIND_TRIALS" "$VALIDATION_TRIALS" "$MAX_TRIAL_ATTEMPTS" "$FIND_RUN_DURATION" "$VALIDATION_RUN_DURATION"; do
        if ! is_positive_int "$v"; then
            die "All numeric controls must be positive integers. Bad value: $v"
        fi
    done

    if (( MIN_THROUGHPUT > MAX_THROUGHPUT )); then
        die "MIN_THROUGHPUT must be <= MAX_THROUGHPUT"
    fi

    mkdir -p "$OUT_DIR"

    local -a steps=()
    if [[ "$#" -gt 0 ]]; then
        for step in "$@"; do
            steps+=("$step")
        done
    else
        while IFS= read -r step; do
            steps+=("$step")
        done < <(default_steps_for_mode)
    fi

    if [[ "${#steps[@]}" -eq 0 ]]; then
        die "No slowdown steps provided"
    fi

    MODE_SUMMARY_CSV="${OUT_DIR}/${SLOWDOWN_TYPE}_summary.csv"
    printf "slowdown_type,step_value,step_label,latency_target_ms,max_target_throughput,observed_throughput_at_max,observed_latency_us_at_max,validated\n" > "$MODE_SUMMARY_CSV"

    log "Search mode=${SLOWDOWN_TYPE}, latency_target=${LATENCY_TARGET_MS}ms, steps=${steps[*]}, find=${FIND_RUN_DURATION}s, validate=${VALIDATION_RUN_DURATION}s x ${VALIDATION_TRIALS}"

    for step in "${steps[@]}"; do
        run_step "$step" || true
    done

    log "Done. Summary: ${MODE_SUMMARY_CSV}"

    if [[ "${#FAILED_STEPS[@]}" -gt 0 ]]; then
        log "Completed with failures: ${FAILED_STEPS[*]}"
        exit 1
    fi
}

main "$@"
