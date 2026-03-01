#!/usr/bin/env bash
# cass.sh
# Run throughput search per slowdown step for a single latency target.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CASS_CLUSTER_SCRIPT="${CASS_CLUSTER_SCRIPT:-${SCRIPT_DIR}/cass-cluster.sh}"
CASS_EXPERIMENT_SCRIPT="${CASS_EXPERIMENT_SCRIPT:-${SCRIPT_DIR}/cass-experiment.sh}"
PROFILE_PLOT_SCRIPT="${PROFILE_PLOT_SCRIPT:-${SCRIPT_DIR}/plot_slowdown_profile.py}"
FLAKY_TIMESERIES_PLOT_SCRIPT="${FLAKY_TIMESERIES_PLOT_SCRIPT:-${SCRIPT_DIR}/plot_flaky_time_series.py}"

SLOWDOWN_TYPE_RAW="${SLOWDOWN_TYPE:-cpu}"
LATENCY_TARGET_MS="${LATENCY_TARGET_MS:-25}"
MODE_KIND="${MODE_KIND:-search}" # search|slowdown_profile

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
PROFILE_TARGET_FACTOR="${PROFILE_TARGET_FACTOR:-0.8}"
PROFILE_FIXED_THROUGHPUT="${PROFILE_FIXED_THROUGHPUT:-}"
PROFILE_STEP_TARGETS="${PROFILE_STEP_TARGETS:-}" # e.g. "4=24000,3=20000,2=16000,1=12000"
PROFILE_TRIALS="${PROFILE_TRIALS:-5}"
PROFILE_WARMUP_TRIALS="${PROFILE_WARMUP_TRIALS:-2}"
PROFILE_RUN_DURATION="${PROFILE_RUN_DURATION:-60}"
FLAKY_FIXED_THROUGHPUT="${FLAKY_FIXED_THROUGHPUT:-}"
FLAKY_TRIALS="${FLAKY_TRIALS:-5}"
FLAKY_RUN_DURATION="${FLAKY_RUN_DURATION:-60}"
FLAKY_UNHEALTHY_CORES="${FLAKY_UNHEALTHY_CORES:-1}"
FLAKY_HEALTHY_INTERVAL_SEC="${FLAKY_HEALTHY_INTERVAL_SEC:-30}"
FLAKY_UNHEALTHY_INTERVAL_SEC="${FLAKY_UNHEALTHY_INTERVAL_SEC:-10}"
FLAKY_NODE="${FLAKY_NODE:-ccl2.cyber.lab}"
FLAKY_END_DROP_RATIO="${FLAKY_END_DROP_RATIO:-0.1}"

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
MODE_TRIALS_CSV=""
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

is_non_negative_int() {
    [[ "$1" =~ ^[0-9]+$ ]]
}

step_target_override() {
    local step="$1"
    local entry
    local key
    local val
    local -a __step_entries=()

    if [[ -z "$PROFILE_STEP_TARGETS" ]]; then
        echo ""
        return
    fi

    IFS=',' read -r -a __step_entries <<< "$PROFILE_STEP_TARGETS"
    for entry in "${__step_entries[@]}"; do
        key="${entry%%=*}"
        val="${entry#*=}"
        key="${key//[[:space:]]/}"
        val="${val//[[:space:]]/}"
        if [[ -z "$key" || -z "$val" || "$entry" != *"="* ]]; then
            continue
        fi
        if [[ "$key" == "$step" ]]; then
            echo "$val"
            return
        fi
    done

    echo ""
}

is_positive_number() {
    awk -v v="$1" 'BEGIN {exit !(v + 0 > 0)}'
}

mean_of() {
    printf '%s\n' "$@" | awk '{sum += $1; n += 1} END {if (n == 0) print "0"; else printf "%.4f", sum / n}'
}

median_of() {
    if [[ "$#" -eq 0 ]]; then
        echo "0"
        return
    fi
    printf '%s\n' "$@" | sort -n | awk '
        {vals[NR] = $1}
        END {
            if (NR == 0) {
                print "0"
            } else if (NR % 2 == 1) {
                printf "%.4f", vals[(NR + 1) / 2]
            } else {
                printf "%.4f", (vals[NR / 2] + vals[(NR / 2) + 1]) / 2.0
            }
        }'
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

default_no_slowdown_step() {
    case "$SLOWDOWN_TYPE" in
        cpu) echo "$DEFAULT_CORES" ;;
        memory) echo "$DEFAULT_MEMORY_MB" ;;
        network_bandwidth) echo "$DEFAULT_BANDWIDTH_MBIT" ;;
        network_latency) echo "$DEFAULT_LATENCY_MS" ;;
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
    local step_trials_csv
    local trial_line
    local trial_phase
    local trial_target
    local trial_idx
    local trial_thr
    local trial_primary_lat
    local trial_mean_lat
    local trial_median_lat
    local trial_p99_lat

    step_label="$(format_step_label "$step")"
    step_dir="${OUT_DIR}/${SLOWDOWN_TYPE}_${step_label}"
    run_log="${step_dir}/run.log"
    step_trials_csv="${step_dir}/search_trials.csv"

    mkdir -p "$step_dir"
    : > "$run_log"

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

    printf "slowdown_type,step_value,step_label,phase,target_throughput,trial_index,observed_throughput_ops_s,primary_latency_us,mean_latency_us,median_latency_us,p99_latency_us\n" > "$step_trials_csv"
    while IFS= read -r trial_line; do
        [[ -n "$trial_line" ]] || continue
        IFS=',' read -r trial_phase trial_target trial_idx trial_thr trial_primary_lat trial_mean_lat trial_median_lat trial_p99_lat <<< "${trial_line#TRIAL_RESULT:}"
        if [[ -z "${trial_phase:-}" || -z "${trial_target:-}" || -z "${trial_idx:-}" || -z "${trial_thr:-}" || -z "${trial_primary_lat:-}" || -z "${trial_mean_lat:-}" || -z "${trial_median_lat:-}" || -z "${trial_p99_lat:-}" ]]; then
            continue
        fi
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
            "$SLOWDOWN_TYPE" "$step" "$step_label" "$trial_phase" "$trial_target" "$trial_idx" "$trial_thr" "$trial_primary_lat" "$trial_mean_lat" "$trial_median_lat" "$trial_p99_lat" >> "$step_trials_csv"
        if [[ -n "$MODE_TRIALS_CSV" ]]; then
            printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
                "$SLOWDOWN_TYPE" "$step" "$step_label" "$trial_phase" "$trial_target" "$trial_idx" "$trial_thr" "$trial_primary_lat" "$trial_mean_lat" "$trial_median_lat" "$trial_p99_lat" >> "$MODE_TRIALS_CSV"
        fi
    done < <(grep '^TRIAL_RESULT:' "$run_log" || true)

    return 0
}

run_baseline_search() {
    local step="$1"
    local baseline_dir="${OUT_DIR}/${SLOWDOWN_TYPE}_baseline"
    local run_log="${baseline_dir}/run.log"
    local max_line
    local rc

    BASELINE_MAX_TARGET="0"
    BASELINE_OBS_THR="0"
    BASELINE_OBS_LAT_US="999999999"
    BASELINE_VALIDATED="false"

    mkdir -p "$baseline_dir"
    : > "$run_log"
    append_log "$run_log" "Starting no-slowdown baseline search (step=${step}) for latency target ${LATENCY_TARGET_MS}ms"

    if [[ "$RESTORE_BEFORE_STEP" == "true" ]]; then
        append_log "$run_log" "Restoring snapshot '${SNAPSHOT_NAME}' before baseline search"
        if ! bash "$CASS_CLUSTER_SCRIPT" snapshot-restore "$SNAPSHOT_NAME" >>"$run_log" 2>&1; then
            append_log "$run_log" "snapshot-restore failed for baseline search"
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
        RUN_TAG="${SLOWDOWN_TYPE}_baseline_lat${LATENCY_TARGET_MS}_$(date +%s)" \
        bash "$CASS_EXPERIMENT_SCRIPT" 2>&1 | tee -a "$run_log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ "$rc" -ne 0 ]]; then
        append_log "$run_log" "Baseline search failed"
        return 1
    fi

    max_line="$(grep '^MAX_RESULT:' "$run_log" | tail -n1 || true)"
    if [[ -n "$max_line" ]]; then
        IFS=',' read -r BASELINE_MAX_TARGET BASELINE_OBS_THR BASELINE_OBS_LAT_US BASELINE_VALIDATED <<< "${max_line#MAX_RESULT:}"
    fi

    if [[ -z "$max_line" || -z "${BASELINE_MAX_TARGET:-}" || -z "${BASELINE_VALIDATED:-}" ]]; then
        append_log "$run_log" "Missing or malformed MAX_RESULT for baseline search"
        return 1
    fi

    append_log "$run_log" "Baseline complete: max_target=${BASELINE_MAX_TARGET}, observed_thr=${BASELINE_OBS_THR}, observed_lat_us=${BASELINE_OBS_LAT_US}, validated=${BASELINE_VALIDATED}"
    return 0
}

run_profile_step() {
    local step="$1"
    local fixed_target="$2"
    local step_label
    local step_dir
    local run_log
    local total_trials
    local rc
    local trial_lines
    local trial_line
    local obs_thr
    local mean_lat
    local median_lat
    local p99_lat
    local trial_phase
    local trial_target
    local trial_idx
    local trial_thr
    local trial_avg_lat
    local trial_mean_lat
    local trial_median_lat
    local trial_p99_lat
    local -a kept_trial_thrs=()
    local -a kept_trial_mean_lats=()
    local -a kept_trial_median_lats=()
    local -a kept_trial_p99_lats=()

    step_label="$(format_step_label "$step")"
    step_dir="${OUT_DIR}/${SLOWDOWN_TYPE}_${step_label}"
    run_log="${step_dir}/run.log"
    total_trials=$((PROFILE_WARMUP_TRIALS + PROFILE_TRIALS))

    mkdir -p "$step_dir"
    : > "$run_log"
    append_log "$run_log" "Starting slowdown-profile step ${step_label}: target throughput ${fixed_target} ops/s, warmup_trials=${PROFILE_WARMUP_TRIALS}, measured_trials=${PROFILE_TRIALS}, total_trials=${total_trials}, duration=${PROFILE_RUN_DURATION}s"

    if [[ "$RESTORE_BEFORE_STEP" == "true" ]]; then
        append_log "$run_log" "Restoring snapshot '${SNAPSHOT_NAME}' before step ${step_label}"
        if ! bash "$CASS_CLUSTER_SCRIPT" snapshot-restore "$SNAPSHOT_NAME" >>"$run_log" 2>&1; then
            append_log "$run_log" "snapshot-restore failed for step ${step_label}"
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
        THROUGHPUT_MODE="single" \
        WORKLOAD_TARGET="$fixed_target" \
        RUN_DURATION="$PROFILE_RUN_DURATION" \
        SINGLE_TRIALS="$total_trials" \
        MAX_TRIAL_ATTEMPTS="$MAX_TRIAL_ATTEMPTS" \
        THREAD_COUNT="$THREAD_COUNT" \
        RECORD_COUNT="$RECORD_COUNT" \
        OPERATION_COUNT="$OPERATION_COUNT" \
        CASS_CONNECTIONS="$CASS_CONNECTIONS" \
        CLIENT_TYPE="$CLIENT_TYPE" \
        SILENCE="$SILENCE" \
        CLEAN_REMOTE_RESULTS="$CLEAN_REMOTE_RESULTS" \
        RUN_TAG="${SLOWDOWN_TYPE}_${step_label}_profile_$(date +%s)" \
        bash "$CASS_EXPERIMENT_SCRIPT" 2>&1 | tee -a "$run_log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ "$rc" -ne 0 ]]; then
        append_log "$run_log" "Profile run failed for step ${step_label}"
        FAILED_STEPS+=("${step_label}:profile")
        return 1
    fi

    trial_lines="$(grep '^TRIAL_RESULT:single,' "$run_log" | tail -n "$PROFILE_TRIALS" || true)"
    if [[ -z "$trial_lines" ]]; then
        append_log "$run_log" "Missing per-trial TRIAL_RESULT lines for step ${step_label} (expected ${PROFILE_TRIALS} measured trials)"
        FAILED_STEPS+=("${step_label}:trial_parse")
        return 1
    fi

    while IFS= read -r trial_line; do
        [[ -n "$trial_line" ]] || continue
        IFS=',' read -r trial_phase trial_target trial_idx trial_thr trial_avg_lat trial_mean_lat trial_median_lat trial_p99_lat <<< "${trial_line#TRIAL_RESULT:}"
        if [[ -z "${trial_idx:-}" || -z "${trial_thr:-}" || -z "${trial_mean_lat:-}" || -z "${trial_median_lat:-}" || -z "${trial_p99_lat:-}" ]]; then
            append_log "$run_log" "Malformed TRIAL_RESULT line for step ${step_label}: ${trial_line}"
            FAILED_STEPS+=("${step_label}:trial_parse")
            return 1
        fi
        kept_trial_thrs+=("$trial_thr")
        kept_trial_mean_lats+=("$trial_mean_lat")
        kept_trial_median_lats+=("$trial_median_lat")
        kept_trial_p99_lats+=("$trial_p99_lat")
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
            "$SLOWDOWN_TYPE" "$step" "$step_label" "$LATENCY_TARGET_MS" "$fixed_target" "$trial_idx" "$trial_thr" "$trial_mean_lat" "$trial_median_lat" "$trial_p99_lat" >> "$MODE_TRIALS_CSV"
    done <<< "$trial_lines"

    if [[ "${#kept_trial_thrs[@]}" -ne "$PROFILE_TRIALS" ]]; then
        append_log "$run_log" "Expected ${PROFILE_TRIALS} measured trials for step ${step_label}, got ${#kept_trial_thrs[@]}"
        FAILED_STEPS+=("${step_label}:trial_count")
        return 1
    fi

    obs_thr="$(mean_of "${kept_trial_thrs[@]}")"
    mean_lat="$(mean_of "${kept_trial_mean_lats[@]}")"
    median_lat="$(median_of "${kept_trial_median_lats[@]}")"
    p99_lat="$(mean_of "${kept_trial_p99_lats[@]}")"

    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$SLOWDOWN_TYPE" "$step" "$step_label" "$LATENCY_TARGET_MS" "$fixed_target" "$obs_thr" "$mean_lat" "$median_lat" "$p99_lat" >> "$MODE_SUMMARY_CSV"

    append_log "$run_log" "Completed step ${step_label}: observed_thr=${obs_thr}, mean_lat_us=${mean_lat}, median_lat_us=${median_lat}, p99_lat_us=${p99_lat} (discarded warmup trials=${PROFILE_WARMUP_TRIALS})"
    return 0
}

run_flaky_cpu() {
    local healthy_cores="$DEFAULT_CORES"
    local step_label="${healthy_cores}c"
    local step_dir="${OUT_DIR}/cpu_flaky_${step_label}"
    local run_log="${step_dir}/run.log"
    local rc
    local ext_line
    local trial_lines
    local trial_line
    local series_lines
    local series_line
    local series_phase
    local series_target
    local series_trial
    local series_time
    local series_thr
    local series_mean_lat
    local series_median_lat
    local obs_thr
    local mean_lat
    local median_lat
    local p99_lat
    local trial_phase
    local trial_target
    local trial_idx
    local trial_thr
    local trial_avg_lat
    local trial_mean_lat
    local trial_median_lat
    local trial_p99_lat
    local flaky_nodes_csv="$FLAKY_NODE"
    local timeseries_samples_csv="${OUT_DIR}/cpu_flaky_timeseries_samples.csv"
    local timeseries_avg_csv="${OUT_DIR}/cpu_flaky_timeseries_avg.csv"
    local throughput_time_plot="${OUT_DIR}/cpu_flaky_throughput_over_time.png"
    local latency_time_plot="${OUT_DIR}/cpu_flaky_latency_over_time.png"
    local tmp_samples_csv
    local tmp_pruned_csv
    tmp_samples_csv="$(mktemp)"
    tmp_pruned_csv="$(mktemp)"

    mkdir -p "$step_dir"
    : > "$run_log"
    append_log "$run_log" "Starting cpu_flaky: healthy_cores=${healthy_cores}, unhealthy_cores=${FLAKY_UNHEALTHY_CORES}, healthy_interval=${FLAKY_HEALTHY_INTERVAL_SEC}s, unhealthy_interval=${FLAKY_UNHEALTHY_INTERVAL_SEC}s, throughput=${FLAKY_FIXED_THROUGHPUT}, trials=${FLAKY_TRIALS}, duration=${FLAKY_RUN_DURATION}s, flaky_node=${flaky_nodes_csv}"

    if [[ "$RESTORE_BEFORE_STEP" == "true" ]]; then
        append_log "$run_log" "Restoring snapshot '${SNAPSHOT_NAME}' before cpu_flaky run"
        if ! bash "$CASS_CLUSTER_SCRIPT" snapshot-restore "$SNAPSHOT_NAME" >>"$run_log" 2>&1; then
            append_log "$run_log" "snapshot-restore failed for cpu_flaky run"
            FAILED_STEPS+=("cpu_flaky:snapshot-restore")
            return 1
        fi
    fi

    set +e
    SLOWDOWN_TYPE="$EXPERIMENT_SLOWDOWN_TYPE" \
        SLOWDOWN_VALUE="$healthy_cores" \
        SLOWDOWN_NODES_CSV="$flaky_nodes_csv" \
        DEFAULT_CORES="$DEFAULT_CORES" \
        DEFAULT_BANDWIDTH_MBIT="$DEFAULT_BANDWIDTH_MBIT" \
        DEFAULT_LATENCY_MS="$DEFAULT_LATENCY_MS" \
        DEFAULT_MEMORY_MB="$DEFAULT_MEMORY_MB" \
        THROUGHPUT_MODE="single" \
        WORKLOAD_TARGET="$FLAKY_FIXED_THROUGHPUT" \
        RUN_DURATION="$FLAKY_RUN_DURATION" \
        SINGLE_TRIALS="$FLAKY_TRIALS" \
        MAX_TRIAL_ATTEMPTS="$MAX_TRIAL_ATTEMPTS" \
        THREAD_COUNT="$THREAD_COUNT" \
        RECORD_COUNT="$RECORD_COUNT" \
        OPERATION_COUNT="$OPERATION_COUNT" \
        CASS_CONNECTIONS="$CASS_CONNECTIONS" \
        CLIENT_TYPE="$CLIENT_TYPE" \
        SILENCE="$SILENCE" \
        CLEAN_REMOTE_RESULTS="$CLEAN_REMOTE_RESULTS" \
        CPU_FLAKY_MODE="true" \
        CPU_HEALTHY_CORES="$healthy_cores" \
        CPU_UNHEALTHY_CORES="$FLAKY_UNHEALTHY_CORES" \
        CPU_HEALTHY_INTERVAL_SEC="$FLAKY_HEALTHY_INTERVAL_SEC" \
        CPU_UNHEALTHY_INTERVAL_SEC="$FLAKY_UNHEALTHY_INTERVAL_SEC" \
        RUN_TAG="cpu_flaky_${step_label}_$(date +%s)" \
        bash "$CASS_EXPERIMENT_SCRIPT" 2>&1 | tee -a "$run_log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ "$rc" -ne 0 ]]; then
        append_log "$run_log" "cpu_flaky run failed"
        FAILED_STEPS+=("cpu_flaky:experiment")
        return 1
    fi

    ext_line="$(grep '^RESULTS_EXT:' "$run_log" | tail -n1 || true)"
    if [[ -z "$ext_line" ]]; then
        append_log "$run_log" "Missing RESULTS_EXT for cpu_flaky run"
        FAILED_STEPS+=("cpu_flaky:parse")
        return 1
    fi
    IFS=',' read -r obs_thr _ mean_lat median_lat p99_lat <<< "${ext_line#RESULTS_EXT:}"
    if [[ -z "${obs_thr:-}" || -z "${mean_lat:-}" || -z "${median_lat:-}" || -z "${p99_lat:-}" ]]; then
        append_log "$run_log" "Malformed RESULTS_EXT for cpu_flaky run: ${ext_line}"
        FAILED_STEPS+=("cpu_flaky:parse")
        return 1
    fi

    printf "mode,healthy_cores,unhealthy_cores,healthy_interval_sec,unhealthy_interval_sec,latency_target_ms,fixed_target_throughput,observed_throughput_ops_s,mean_latency_us,median_latency_us,p99_latency_us\n" > "$MODE_SUMMARY_CSV"
    printf "cpu_flaky,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$healthy_cores" "$FLAKY_UNHEALTHY_CORES" "$FLAKY_HEALTHY_INTERVAL_SEC" "$FLAKY_UNHEALTHY_INTERVAL_SEC" "$LATENCY_TARGET_MS" "$FLAKY_FIXED_THROUGHPUT" "$obs_thr" "$mean_lat" "$median_lat" "$p99_lat" >> "$MODE_SUMMARY_CSV"

    trial_lines="$(grep '^TRIAL_RESULT:single,' "$run_log" | tail -n "$FLAKY_TRIALS" || true)"
    if [[ -z "$trial_lines" ]]; then
        append_log "$run_log" "Missing per-trial TRIAL_RESULT lines for cpu_flaky run"
        FAILED_STEPS+=("cpu_flaky:trial_parse")
        return 1
    fi

    printf "mode,healthy_cores,unhealthy_cores,healthy_interval_sec,unhealthy_interval_sec,latency_target_ms,fixed_target_throughput,trial_index,observed_throughput_ops_s,mean_latency_us,median_latency_us,p99_latency_us\n" > "$MODE_TRIALS_CSV"
    while IFS= read -r trial_line; do
        [[ -n "$trial_line" ]] || continue
        IFS=',' read -r trial_phase trial_target trial_idx trial_thr trial_avg_lat trial_mean_lat trial_median_lat trial_p99_lat <<< "${trial_line#TRIAL_RESULT:}"
        if [[ -z "${trial_idx:-}" || -z "${trial_thr:-}" || -z "${trial_mean_lat:-}" || -z "${trial_median_lat:-}" || -z "${trial_p99_lat:-}" ]]; then
            append_log "$run_log" "Malformed TRIAL_RESULT line for cpu_flaky run: ${trial_line}"
            FAILED_STEPS+=("cpu_flaky:trial_parse")
            return 1
        fi
        printf "cpu_flaky,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
            "$healthy_cores" "$FLAKY_UNHEALTHY_CORES" "$FLAKY_HEALTHY_INTERVAL_SEC" "$FLAKY_UNHEALTHY_INTERVAL_SEC" "$LATENCY_TARGET_MS" "$FLAKY_FIXED_THROUGHPUT" "$trial_idx" "$trial_thr" "$trial_mean_lat" "$trial_median_lat" "$trial_p99_lat" >> "$MODE_TRIALS_CSV"
    done <<< "$trial_lines"

    series_lines="$(grep '^TRIAL_SERIES:single,' "$run_log" || true)"
    if [[ -z "$series_lines" ]]; then
        append_log "$run_log" "Missing TRIAL_SERIES lines for cpu_flaky run"
        FAILED_STEPS+=("cpu_flaky:timeseries_parse")
        return 1
    fi
    printf "mode,trial_index,time_sec,throughput_ops_s,mean_latency_us,median_latency_us\n" > "$tmp_samples_csv"
    while IFS= read -r series_line; do
        [[ -n "$series_line" ]] || continue
        IFS=',' read -r series_phase series_target series_trial series_time series_thr series_mean_lat series_median_lat <<< "${series_line#TRIAL_SERIES:}"
        if [[ -z "${series_trial:-}" || -z "${series_time:-}" || -z "${series_thr:-}" || -z "${series_mean_lat:-}" || -z "${series_median_lat:-}" ]]; then
            append_log "$run_log" "Malformed TRIAL_SERIES line for cpu_flaky run: ${series_line}"
            FAILED_STEPS+=("cpu_flaky:timeseries_parse")
            return 1
        fi
        printf "cpu_flaky,%s,%s,%s,%s,%s\n" \
            "$series_trial" "$series_time" "$series_thr" "$series_mean_lat" "$series_median_lat" >> "$tmp_samples_csv"
    done <<< "$series_lines"

    awk -F, -v fixed_target="$FLAKY_FIXED_THROUGHPUT" -v drop_ratio="$FLAKY_END_DROP_RATIO" '
        NR == 1 { next }
        {
            trial = $2 + 0
            idx = ++seq[trial]
            row[trial, idx] = $0
            thr = $4 + 0
            mean = $5 + 0
            med = $6 + 0
            min_thr = fixed_target * drop_ratio
            if (thr > 0 || mean > 0 || med > 0) {
                last_nz[trial] = idx
            }
            if (thr >= min_thr) {
                last_good[trial] = idx
            }
            if (!(trial in seen)) {
                seen[trial] = 1
                trials[++trial_count] = trial
            }
        }
        END {
            for (i = 1; i <= trial_count; i++) {
                trial = trials[i]
                limit = last_good[trial] + 0
                if (limit <= 0) {
                    limit = last_nz[trial] + 0
                }
                if (limit <= 0) {
                    continue
                }
                for (j = 1; j <= limit; j++) {
                    print row[trial, j]
                }
            }
        }' "$tmp_samples_csv" > "$tmp_pruned_csv"

    {
        printf "mode,trial_index,time_sec,throughput_ops_s,mean_latency_us,median_latency_us\n"
        sort -t, -k2,2n -k3,3n "$tmp_pruned_csv"
    } > "$timeseries_samples_csv"

    rm -f "$tmp_samples_csv" "$tmp_pruned_csv"

    if ! python3 "$FLAKY_TIMESERIES_PLOT_SCRIPT" \
        --input-samples-csv "$timeseries_samples_csv" \
        --output-avg-csv "$timeseries_avg_csv" \
        --throughput-plot-out "$throughput_time_plot" \
        --latency-plot-out "$latency_time_plot"; then
        append_log "$run_log" "Failed to build cpu_flaky time-series plots/avg CSV"
        FAILED_STEPS+=("cpu_flaky:timeseries_plot")
        return 1
    fi

    append_log "$run_log" "Completed cpu_flaky run: observed_thr=${obs_thr}, mean_lat_us=${mean_lat}, median_lat_us=${median_lat}, p99_lat_us=${p99_lat}"
    append_log "$run_log" "Time-series outputs: samples=${timeseries_samples_csv}, avg=${timeseries_avg_csv}, throughput_plot=${throughput_time_plot}, latency_plot=${latency_time_plot}"
    return 0
}

main() {
    local step
    local no_slowdown_step
    local baseline_target_for_log="skipped"
    local fixed_target_raw
    local fixed_target

    SLOWDOWN_TYPE="$(normalize_slowdown_type "${SLOWDOWN_TYPE_RAW}")" || die "Unsupported SLOWDOWN_TYPE='${SLOWDOWN_TYPE_RAW}'"
    EXPERIMENT_SLOWDOWN_TYPE="$(experiment_slowdown_type)"

    for v in "$MIN_THROUGHPUT" "$INITIAL_THROUGHPUT" "$MAX_THROUGHPUT" "$THROUGHPUT_GRANULARITY" "$MAX_EVAL_POINTS" "$FIND_TRIALS" "$VALIDATION_TRIALS" "$MAX_TRIAL_ATTEMPTS" "$FIND_RUN_DURATION" "$VALIDATION_RUN_DURATION" "$PROFILE_TRIALS" "$PROFILE_RUN_DURATION" "$FLAKY_TRIALS" "$FLAKY_RUN_DURATION" "$FLAKY_UNHEALTHY_CORES" "$FLAKY_HEALTHY_INTERVAL_SEC" "$FLAKY_UNHEALTHY_INTERVAL_SEC"; do
        if ! is_positive_int "$v"; then
            die "All numeric controls must be positive integers. Bad value: $v"
        fi
    done
    if ! is_non_negative_int "$PROFILE_WARMUP_TRIALS"; then
        die "PROFILE_WARMUP_TRIALS must be a non-negative integer"
    fi

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

    case "$MODE_KIND" in
        search)
            MODE_SUMMARY_CSV="${OUT_DIR}/${SLOWDOWN_TYPE}_summary.csv"
            MODE_TRIALS_CSV="${OUT_DIR}/${SLOWDOWN_TYPE}_search_trials.csv"
            printf "slowdown_type,step_value,step_label,latency_target_ms,max_target_throughput,observed_throughput_at_max,observed_latency_us_at_max,validated\n" > "$MODE_SUMMARY_CSV"
            printf "slowdown_type,step_value,step_label,phase,target_throughput,trial_index,observed_throughput_ops_s,primary_latency_us,mean_latency_us,median_latency_us,p99_latency_us\n" > "$MODE_TRIALS_CSV"

            log "Search mode=${SLOWDOWN_TYPE}, latency_target=${LATENCY_TARGET_MS}ms, steps=${steps[*]}, find=${FIND_RUN_DURATION}s, validate=${VALIDATION_RUN_DURATION}s x ${VALIDATION_TRIALS}"

            for step in "${steps[@]}"; do
                run_step "$step" || true
            done

            log "Done. Summary: ${MODE_SUMMARY_CSV}; trials: ${MODE_TRIALS_CSV}"
            ;;
        slowdown_profile)
            if ! is_positive_number "$PROFILE_TARGET_FACTOR"; then
                die "PROFILE_TARGET_FACTOR must be > 0"
            fi
            if [[ -n "$PROFILE_STEP_TARGETS" ]]; then
                log "Slowdown-profile using per-step throughput targets (${PROFILE_STEP_TARGETS}); skipping baseline find/validate"
            elif [[ -n "$PROFILE_FIXED_THROUGHPUT" ]]; then
                if ! is_positive_int "$PROFILE_FIXED_THROUGHPUT"; then
                    die "PROFILE_FIXED_THROUGHPUT must be a positive integer"
                fi
                fixed_target="$PROFILE_FIXED_THROUGHPUT"
                if (( fixed_target < MIN_THROUGHPUT )); then
                    fixed_target="$MIN_THROUGHPUT"
                fi
                if (( fixed_target > MAX_THROUGHPUT )); then
                    fixed_target="$MAX_THROUGHPUT"
                fi
                log "Slowdown-profile using explicit fixed target throughput=${fixed_target}; skipping baseline find/validate"
            else
                no_slowdown_step="$(default_no_slowdown_step)"
                run_baseline_search "$no_slowdown_step" || {
                    FAILED_STEPS+=("baseline:search")
                    die "Baseline no-slowdown search failed"
                }
                if [[ "$BASELINE_VALIDATED" != "true" ]]; then
                    FAILED_STEPS+=("baseline:not_validated")
                    die "Baseline search did not validate; cannot derive fixed target"
                fi

                fixed_target_raw="$(awk -v t="$BASELINE_MAX_TARGET" -v f="$PROFILE_TARGET_FACTOR" 'BEGIN {printf "%.0f", t * f}')"
                if [[ -z "$fixed_target_raw" ]] || ! [[ "$fixed_target_raw" =~ ^[0-9]+$ ]]; then
                    die "Failed to calculate fixed target throughput from baseline=${BASELINE_MAX_TARGET}, factor=${PROFILE_TARGET_FACTOR}"
                fi
                if (( fixed_target_raw < MIN_THROUGHPUT )); then
                    fixed_target_raw="$MIN_THROUGHPUT"
                fi
                fixed_target=$(((fixed_target_raw / THROUGHPUT_GRANULARITY) * THROUGHPUT_GRANULARITY))
                if (( fixed_target < MIN_THROUGHPUT )); then
                    fixed_target="$MIN_THROUGHPUT"
                fi
                if (( fixed_target > MAX_THROUGHPUT )); then
                    fixed_target="$MAX_THROUGHPUT"
                fi
                baseline_target_for_log="$BASELINE_MAX_TARGET"
            fi

            MODE_SUMMARY_CSV="${OUT_DIR}/${SLOWDOWN_TYPE}_slowdown_profile.csv"
            printf "slowdown_type,step_value,step_label,latency_target_ms,fixed_target_throughput,observed_throughput_ops_s,mean_latency_us,median_latency_us,p99_latency_us\n" > "$MODE_SUMMARY_CSV"
            MODE_TRIALS_CSV="${OUT_DIR}/${SLOWDOWN_TYPE}_slowdown_profile_trials.csv"
            printf "slowdown_type,step_value,step_label,latency_target_ms,fixed_target_throughput,trial_index,observed_throughput_ops_s,mean_latency_us,median_latency_us,p99_latency_us\n" > "$MODE_TRIALS_CSV"

            log "Slowdown-profile mode=${SLOWDOWN_TYPE}, latency_target=${LATENCY_TARGET_MS}ms, baseline_target=${baseline_target_for_log}, fixed_target=${fixed_target:-per-step}, warmup_trials=${PROFILE_WARMUP_TRIALS}, measured_trials=${PROFILE_TRIALS}, duration=${PROFILE_RUN_DURATION}s, steps=${steps[*]}"

            for step in "${steps[@]}"; do
                local step_target
                if [[ -n "$PROFILE_STEP_TARGETS" ]]; then
                    step_target="$(step_target_override "$step")"
                    if [[ -z "$step_target" ]]; then
                        FAILED_STEPS+=("${step}:missing_step_target")
                        append_log "${OUT_DIR}/${SLOWDOWN_TYPE}_profile.log" "Missing throughput target for step ${step}. Set PROFILE_STEP_TARGETS like '4=24000,3=20000,...'"
                        continue
                    fi
                    if ! is_positive_int "$step_target"; then
                        FAILED_STEPS+=("${step}:bad_step_target")
                        append_log "${OUT_DIR}/${SLOWDOWN_TYPE}_profile.log" "Invalid throughput target '${step_target}' for step ${step}. Must be positive integer."
                        continue
                    fi
                    if (( step_target < MIN_THROUGHPUT )); then
                        step_target="$MIN_THROUGHPUT"
                    fi
                    if (( step_target > MAX_THROUGHPUT )); then
                        step_target="$MAX_THROUGHPUT"
                    fi
                else
                    step_target="$fixed_target"
                fi
                run_profile_step "$step" "$step_target" || true
            done

            if [[ -x "$PROFILE_PLOT_SCRIPT" || -f "$PROFILE_PLOT_SCRIPT" ]]; then
                local throughput_plot="${OUT_DIR}/${SLOWDOWN_TYPE}_slowdown_profile_throughput.png"
                local latency_plot="${OUT_DIR}/${SLOWDOWN_TYPE}_slowdown_profile_latency.png"
                if python3 "$PROFILE_PLOT_SCRIPT" \
                    --input-csv "$MODE_SUMMARY_CSV" \
                    --mode "$SLOWDOWN_TYPE" \
                    --throughput-plot-out "$throughput_plot" \
                    --latency-plot-out "$latency_plot"; then
                    log "Built slowdown-profile plots: ${throughput_plot}, ${latency_plot}"
                else
                    log "WARNING: Failed to build slowdown-profile plots with ${PROFILE_PLOT_SCRIPT}"
                fi
            fi

            log "Done. Summary: ${MODE_SUMMARY_CSV}; trials: ${MODE_TRIALS_CSV}"
            ;;
        cpu_flaky)
            if [[ "$SLOWDOWN_TYPE" != "cpu" ]]; then
                die "MODE_KIND=cpu_flaky requires SLOWDOWN_TYPE=cpu"
            fi
            if ! is_positive_int "$FLAKY_FIXED_THROUGHPUT"; then
                die "FLAKY_FIXED_THROUGHPUT must be set to a positive integer"
            fi
            if (( FLAKY_UNHEALTHY_CORES > DEFAULT_CORES )); then
                die "FLAKY_UNHEALTHY_CORES must be <= DEFAULT_CORES"
            fi

            MODE_SUMMARY_CSV="${OUT_DIR}/cpu_flaky_summary.csv"
            MODE_TRIALS_CSV="${OUT_DIR}/cpu_flaky_trials.csv"
            log "CPU flaky mode: healthy_cores=${DEFAULT_CORES}, unhealthy_cores=${FLAKY_UNHEALTHY_CORES}, healthy_interval=${FLAKY_HEALTHY_INTERVAL_SEC}s, unhealthy_interval=${FLAKY_UNHEALTHY_INTERVAL_SEC}s, throughput=${FLAKY_FIXED_THROUGHPUT}, trials=${FLAKY_TRIALS}, duration=${FLAKY_RUN_DURATION}s"
            run_flaky_cpu || true
            log "Done. Summary: ${MODE_SUMMARY_CSV}; trials: ${MODE_TRIALS_CSV}"
            ;;
        *)
            die "Unsupported MODE_KIND='${MODE_KIND}' (expected search|slowdown_profile|cpu_flaky)"
            ;;
    esac

    if [[ "${#FAILED_STEPS[@]}" -gt 0 ]]; then
        log "Completed with failures: ${FAILED_STEPS[*]}"
        exit 1
    fi
}

main "$@"
