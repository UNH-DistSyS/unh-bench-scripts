#!/usr/bin/env bash
#
# cass.sh
#
# Top-level orchestrator for Cassandra slowdown experiments.

set -euo pipefail

# -----------------------------
# Config
# -----------------------------

SSH_USER="${SSH_USER:-$USER}"
SSH_OPTS="${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"}"
BENCH_HOST="${BENCH_HOST:-ccl4.cyber.lab}"
BENCH_USER="${BENCH_USER:-$SSH_USER}"

# Slowdown mode: cpu, network, latency, or memory
SLOWDOWN_TYPE="${SLOWDOWN_TYPE:-cpu}"

CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"

# Number of independent trials per slowdown step
TRIALS="${TRIALS:-1}"
# Number of retries for a trial if the experiment run fails validation
MAX_TRIAL_ATTEMPTS="${MAX_TRIAL_ATTEMPTS:-3}"

# Default CPU core counts if none are given on CLI
CPU_STEPS_DEFAULT=("8" "6" "4" "2" "1")
NETWORK_STEPS_DEFAULT=("1000" "750" "500" "250" "100")
LATENCY_STEPS_DEFAULT=("10us" "100us" "1ms" "10ms" "100ms")

# Max cores for non-slowdown nodes
DEFAULT_CORES="${DEFAULT_CORES:-8}"
DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT:-1000}"
DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS:-0ms}"

# Bench host load generation defaults. If THREAD_COUNT / WORKLOAD_TARGET are not
# set in the environment, these are auto-derived from ccl4 CPU count.
BENCH_THREAD_MULTIPLIER="${BENCH_THREAD_MULTIPLIER:-16}"
BENCH_TARGET_PER_THREAD="${BENCH_TARGET_PER_THREAD:-1000}"

# Cassandra driver connections per host.
CASS_CONNECTIONS="${CASS_CONNECTIONS:-8}"

# Default target available memory (MB) for non-slowdown nodes.
# In memory mode, the slowed node reserves (DEFAULT_MEMORY_MB - step_value) in /dev/shm.
DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB:-49152}"

# Nodes that should be slowed down (subset of BENCH_SYSTEMS)
SLOWDOWN_NODES=("ccl1.cyber.lab")

# Paths (relative to this script dir by default)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASS_CLUSTER_SCRIPT="${CASS_CLUSTER_SCRIPT:-${SCRIPT_DIR}/cass-cluster.sh}"
CASS_EXPERIMENT_SCRIPT="${CASS_EXPERIMENT_SCRIPT:-${SCRIPT_DIR}/cass-experiment.sh}"
PROCESS_PY_SCRIPT="${PROCESS_PY_SCRIPT:-${SCRIPT_DIR}/process_cass_values.py}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/out}"
DATA_DIR="${DATA_DIR:-${OUT_DIR}/data}"
GRAPHS_DIR="${GRAPHS_DIR:-${OUT_DIR}/graphs}"

SLOWDOWN_PLOT_TAG=""
case "$SLOWDOWN_TYPE" in
    cpu) SLOWDOWN_PLOT_TAG="cores" ;;
    memory) SLOWDOWN_PLOT_TAG="memory" ;;
    network) SLOWDOWN_PLOT_TAG="network_bandwidth" ;;
    latency) SLOWDOWN_PLOT_TAG="network_latency" ;;
    *) SLOWDOWN_PLOT_TAG="$SLOWDOWN_TYPE" ;;
esac

TRIALS_CSV="${TRIALS_CSV:-${DATA_DIR}/cass_${SLOWDOWN_TYPE}_trials_${RUN_ID}.csv}"
SUMMARY_CSV="${SUMMARY_CSV:-${DATA_DIR}/cass_${SLOWDOWN_TYPE}_summary_${RUN_ID}.csv}"
NORM_PLOT_OUT="${NORM_PLOT_OUT:-${GRAPHS_DIR}/cassandra_normalized_throughput_degrading_by_${SLOWDOWN_PLOT_TAG}.png}"
THROUGHPUT_PLOT_OUT="${THROUGHPUT_PLOT_OUT:-${GRAPHS_DIR}/cassandra_throughput_degrading_by_${SLOWDOWN_PLOT_TAG}.png}"
LATENCY_PLOT_OUT="${LATENCY_PLOT_OUT:-${GRAPHS_DIR}/cassandra_latency_degrading_by_${SLOWDOWN_PLOT_TAG}.png}"
RELATIVE_PLOT_OUT="${RELATIVE_PLOT_OUT:-${GRAPHS_DIR}/cassandra_relative_slowdown_degrading_by_${SLOWDOWN_PLOT_TAG}.png}"

log() {
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "[$ts] $*"
}

die() {
    log "ERROR: $*"
    exit 1
}

is_positive_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

latency_to_us() {
    local val="$1"
    case "$val" in
        *us) awk -v v="${val%us}" 'BEGIN {printf "%.6f", v}' ;;
        *ms) awk -v v="${val%ms}" 'BEGIN {printf "%.6f", v * 1000.0}' ;;
        *s)  awk -v v="${val%s}" 'BEGIN {printf "%.6f", v * 1000000.0}' ;;
        *)   awk -v v="$val" 'BEGIN {printf "%.6f", v * 1000.0}' ;;
    esac
}

detect_bench_cores() {
    local cores=""
    cores="$(ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" "nproc" 2>/dev/null || true)"
    if [[ -n "$cores" ]] && is_positive_int "$cores"; then
        echo "$cores"
        return 0
    fi
    return 1
}

configure_bench_load() {
    local bench_cores=""
    bench_cores="$(detect_bench_cores || true)"

    if [[ -z "${THREAD_COUNT:-}" ]]; then
        if [[ -n "$bench_cores" ]]; then
            THREAD_COUNT="$((bench_cores * BENCH_THREAD_MULTIPLIER))"
        else
            THREAD_COUNT="512"
        fi
    fi

    if [[ -z "${WORKLOAD_TARGET:-}" ]]; then
        if ! is_positive_int "$THREAD_COUNT"; then
            die "THREAD_COUNT must be a positive integer (got '$THREAD_COUNT')."
        fi
        WORKLOAD_TARGET="$((THREAD_COUNT * BENCH_TARGET_PER_THREAD))"
    fi

    if ! is_positive_int "$THREAD_COUNT"; then
        die "THREAD_COUNT must be a positive integer (got '$THREAD_COUNT')."
    fi
    if ! is_positive_int "$WORKLOAD_TARGET"; then
        die "WORKLOAD_TARGET must be a positive integer (got '$WORKLOAD_TARGET')."
    fi

    export THREAD_COUNT WORKLOAD_TARGET

    if [[ -n "$bench_cores" ]]; then
        log "Bench host $BENCH_HOST has ${bench_cores} CPUs; using THREAD_COUNT=${THREAD_COUNT}, WORKLOAD_TARGET=${WORKLOAD_TARGET}"
    else
        log "Could not detect bench host CPU count; using THREAD_COUNT=${THREAD_COUNT}, WORKLOAD_TARGET=${WORKLOAD_TARGET}"
    fi
}

calc_slowdown_pct() {
    local current="$1"
    local baseline="$2"
    if [[ "$SLOWDOWN_TYPE" == "latency" ]]; then
        local c_us b_us m_us
        c_us="$(latency_to_us "$current")"
        b_us="$(latency_to_us "$baseline")"
        m_us="$(latency_to_us "${LATENCY_MAX_STEP:-$current}")"
        awk -v c="$c_us" -v b="$b_us" -v m="$m_us" 'BEGIN {
            if (m <= b) {
                print "N/A"
                exit
            }
            pct = ((c - b) / (m - b)) * 100.0
            if (pct < 0) pct = 0
            if (pct > 100) pct = 100
            printf "%.2f", pct
        }'
        return
    fi

    awk -v c="$current" -v b="$baseline" 'BEGIN {
        if (b <= 0) {
            print "N/A"
            exit
        }
        pct = ((b - c) / b) * 100.0
        if (pct < 0) pct = 0
        if (pct > 100) pct = 100
        printf "%.2f", pct
    }'
}

calc_mean() {
    printf "%s\n" "$@" | awk '
        $1 != "" && $1 != "N/A" {sum += $1; n += 1}
        END {
            if (n > 0) printf "%.4f", sum / n
            else print "N/A"
        }'
}

calc_stddev() {
    printf "%s\n" "$@" | awk '
        $1 != "" && $1 != "N/A" {vals[n++] = $1; sum += $1}
        END {
            if (n < 2) {
                print "0.0000"
                exit
            }
            mean = sum / n
            for (i = 0; i < n; i++) {
                diff = vals[i] - mean
                var += diff * diff
            }
            printf "%.4f", sqrt(var / n)
        }'
}

default_steps_for_mode() {
    case "$SLOWDOWN_TYPE" in
        cpu)
            printf "%s\n" "${CPU_STEPS_DEFAULT[@]}"
            ;;
        network)
            printf "%s\n" "${NETWORK_STEPS_DEFAULT[@]}"
            ;;
        latency)
            printf "%s\n" "${LATENCY_STEPS_DEFAULT[@]}"
            ;;
        memory)
            local q3=$((DEFAULT_MEMORY_MB * 3 / 4))
            local q2=$((DEFAULT_MEMORY_MB / 2))
            local q1=$((DEFAULT_MEMORY_MB / 4))
            if (( q3 < 128 )); then q3=128; fi
            if (( q2 < 128 )); then q2=128; fi
            if (( q1 < 128 )); then q1=128; fi
            printf "%s\n" "$DEFAULT_MEMORY_MB" "$q3" "$q2" "$q1"
            ;;
        *)
            die "Unsupported SLOWDOWN_TYPE '$SLOWDOWN_TYPE' (expected: cpu, network, latency, or memory)"
            ;;
    esac
}

step_label_for_mode() {
    local step_value="$1"
    case "$SLOWDOWN_TYPE" in
        cpu) echo "${step_value} cores" ;;
        network) echo "${step_value} mbit" ;;
        latency) echo "${step_value}" ;;
        memory) echo "${step_value} MB" ;;
        *) echo "$step_value" ;;
    esac
}

# -----------------------------
# Determine slowdown steps to run
# -----------------------------

if ! is_positive_int "$TRIALS"; then
    die "TRIALS must be a positive integer (got '$TRIALS')."
fi
if ! is_positive_int "$MAX_TRIAL_ATTEMPTS"; then
    die "MAX_TRIAL_ATTEMPTS must be a positive integer (got '$MAX_TRIAL_ATTEMPTS')."
fi

configure_bench_load

mkdir -p "$OUT_DIR" "$DATA_DIR" "$GRAPHS_DIR"

STEPS=()

if [[ $# -gt 0 ]]; then
    # Use step values provided on the command line
    for v in "$@"; do
        STEPS+=("$v")
    done
else
    # Use defaults for slowdown type
    while IFS= read -r step; do
        STEPS+=("$step")
    done < <(default_steps_for_mode)
fi

if [[ "${#STEPS[@]}" -eq 0 ]]; then
    die "No slowdown step values specified."
fi

case "$SLOWDOWN_TYPE" in
    cpu) BASELINE_VALUE="$DEFAULT_CORES" ;;
    network) BASELINE_VALUE="$DEFAULT_BANDWIDTH_MBIT" ;;
    latency) BASELINE_VALUE="$DEFAULT_LATENCY_MS" ;;
    memory) BASELINE_VALUE="$DEFAULT_MEMORY_MB" ;;
    *) die "Unsupported SLOWDOWN_TYPE '$SLOWDOWN_TYPE' (expected: cpu, network, latency, or memory)" ;;
esac

if [[ "$SLOWDOWN_TYPE" == "latency" ]]; then
    LATENCY_MAX_STEP=""
    LATENCY_MAX_US="0"
    for v in "${STEPS[@]}"; do
        v_us="$(latency_to_us "$v")"
        if [[ -z "$LATENCY_MAX_STEP" ]] || awk -v a="$v_us" -v b="$LATENCY_MAX_US" 'BEGIN {exit !(a > b)}'; then
            LATENCY_MAX_STEP="$v"
            LATENCY_MAX_US="$v_us"
        fi
    done
fi

log "Slowdown type: $SLOWDOWN_TYPE"
log "Baseline value on non-slowdown nodes: $BASELINE_VALUE"
log "Steps to test: ${STEPS[*]}"
log "Trials per step: $TRIALS"
log "Max attempts per trial: $MAX_TRIAL_ATTEMPTS"

# -----------------------------
# Run experiments
# -----------------------------

SLOWDOWN_PCTS=()
STEP_VALUES=()
THROUGHPUTS=()
LATENCIES=()

slowdown_nodes_csv=""
if [[ "${#SLOWDOWN_NODES[@]}" -gt 0 ]]; then
    slowdown_nodes_csv="$(IFS=,; echo "${SLOWDOWN_NODES[*]}")"
fi

mkdir -p "$(dirname "$TRIALS_CSV")" "$(dirname "$SUMMARY_CSV")"

printf "slowdown_type,step_value,slowdown_pct,trial,throughput,latency\n" > "$TRIALS_CSV"
printf "slowdown_type,step_value,slowdown_pct,trials,throughput_avg,throughput_stddev,latency_avg,latency_stddev\n" > "$SUMMARY_CSV"

for step_value in "${STEPS[@]}"; do
    step_slowdown_pct="$(calc_slowdown_pct "$step_value" "$BASELINE_VALUE")"
    STEP_THRS=()
    STEP_LATS=()

    log "==============================================="
    log "Starting $SLOWDOWN_TYPE experiment for step value ${step_value} (slowdown=${step_slowdown_pct}%)"
    log "==============================================="

    for trial in $(seq 1 "$TRIALS"); do
        log "---- Trial ${trial}/${TRIALS} for step ${step_value} ----"
        output=""
        trial_succeeded=0
        for attempt in $(seq 1 "$MAX_TRIAL_ATTEMPTS"); do
            log "Trial ${trial} attempt ${attempt}/${MAX_TRIAL_ATTEMPTS}"

            # Bring up a new cluster (cass-cluster.sh up already tears down)
            log "Bringing up a fresh cluster..."
            bash "$CASS_CLUSTER_SCRIPT" up

            log "Cluster is up. Running trial with value ${step_value}..."
            if output=$(CLIENT_TYPE="${CLIENT_TYPE}" \
                SLOWDOWN_TYPE="${SLOWDOWN_TYPE}" SLOWDOWN_VALUE="${step_value}" CONTAINER_CORES="${step_value}" \
                DEFAULT_CORES="${DEFAULT_CORES}" DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT}" DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS}" DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB}" \
                SLOWDOWN_NODES_CSV="${slowdown_nodes_csv}" CASS_CONNECTIONS="${CASS_CONNECTIONS}" \
                bash "$CASS_EXPERIMENT_SCRIPT" "$step_value" 2>&1); then
                trial_succeeded=1
                break
            fi

            log "Attempt ${attempt} failed for step ${step_value}, trial ${trial}."
            log "$output"
            if [[ "$attempt" -lt "$MAX_TRIAL_ATTEMPTS" ]]; then
                log "Retrying trial ${trial} for step ${step_value} with a fresh cluster."
            fi
        done

        if [[ "$trial_succeeded" -ne 1 ]]; then
            die "All ${MAX_TRIAL_ATTEMPTS} attempts failed for step ${step_value}, trial ${trial}."
        fi

        # Log the output
        log "Experiment output for step ${step_value}, trial ${trial}:"
        log "$output"

        # Grab the "RESULTS:value,throughput,latency" line
        result_line=$(echo "$output" | grep "^RESULTS:${step_value},")
        if [[ -z "$result_line" ]]; then
            die "Failed to find RESULTS line in output for step ${step_value}, trial ${trial}."
        fi
        IFS=',' read -r result_value throughput latency <<< "${result_line#RESULTS:}"

        STEP_THRS+=("$throughput")
        STEP_LATS+=("$latency")
        printf "%s,%s,%s,%s,%s,%s\n" \
            "$SLOWDOWN_TYPE" "$result_value" "$step_slowdown_pct" "$trial" "$throughput" "$latency" >> "$TRIALS_CSV"

        log "Recorded trial ${trial}/${TRIALS}: throughput=${throughput}, latency=${latency}"

        # Tear down cluster after this trial
        log "Tearing down cluster after trial ${trial}..."
        if ! bash "$CASS_CLUSTER_SCRIPT" down; then
            log "WARNING: cass-cluster.sh down returned non-zero after trial."
        fi
    done

    step_thr_avg="$(calc_mean "${STEP_THRS[@]}")"
    step_lat_avg="$(calc_mean "${STEP_LATS[@]}")"
    step_thr_stddev="$(calc_stddev "${STEP_THRS[@]}")"
    step_lat_stddev="$(calc_stddev "${STEP_LATS[@]}")"

    SLOWDOWN_PCTS+=("$step_slowdown_pct")
    STEP_VALUES+=("$step_value")
    THROUGHPUTS+=("$step_thr_avg")
    LATENCIES+=("$step_lat_avg")

    printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$SLOWDOWN_TYPE" "$step_value" "$step_slowdown_pct" "$TRIALS" \
        "$step_thr_avg" "$step_thr_stddev" "$step_lat_avg" "$step_lat_stddev" >> "$SUMMARY_CSV"

    log "Completed step ${step_value}: throughput_avg=${step_thr_avg} (stddev=${step_thr_stddev}), latency_avg=${step_lat_avg} (stddev=${step_lat_stddev})"
done

# -----------------------------
# Call process_cass_values.py
# -----------------------------

log "All experiments complete."
log "Trial data written to: $TRIALS_CSV"
log "Summary data written to: $SUMMARY_CSV"
log "Plots will be written to:"
log "  - $NORM_PLOT_OUT"
log "  - $RELATIVE_PLOT_OUT"
log "  - $THROUGHPUT_PLOT_OUT"
log "  - $LATENCY_PLOT_OUT"
log "Calling process_cass_values.py ..."

ARGS=()
for i in "${!SLOWDOWN_PCTS[@]}"; do
    slowdown_pct="${SLOWDOWN_PCTS[$i]}"
    step_value="${STEP_VALUES[$i]}"
    thr="${THROUGHPUTS[$i]}"
    lat="${LATENCIES[$i]}"
    label="$(step_label_for_mode "$step_value")"

    # process_cass_values expects: cpu_pct,avg_throughput,avg_latency
    ARGS+=( "$(printf "%s,%s,%s,%s" "$slowdown_pct" "$thr" "$lat" "$label")" )
    log "Final point: mode=${SLOWDOWN_TYPE}, value=${step_value}, slowdown=${slowdown_pct}%, throughput=${thr}, latency=${lat}"
done

# You can pass --format table / --format json / --plot-out, etc.
python3 "$PROCESS_PY_SCRIPT" \
    --plot-out "$NORM_PLOT_OUT" \
    --relative-plot-out "$RELATIVE_PLOT_OUT" \
    --throughput-plot-out "$THROUGHPUT_PLOT_OUT" \
    --latency-plot-out "$LATENCY_PLOT_OUT" \
    "${ARGS[@]}"
