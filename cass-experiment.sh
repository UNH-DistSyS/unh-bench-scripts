#!/usr/bin/env bash

set -euo pipefail

SSH_OPTS="${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"}"
BENCH_HOST="${BENCH_HOST:-ccl4.cyber.lab}"
BENCH_USER="${BENCH_USER:-$USER}"
BENCH_DIR_ABSOLUTE="${BENCH_DIR_ABSOLUTE:-/home/$BENCH_USER/UNHBench}"
RESULTS_DIR_BASE="${RESULTS_DIR_BASE:-/home/$BENCH_USER/cass-results}"

BENCH_SYSTEMS_CSV="${BENCH_SYSTEMS_CSV:-ccl1.cyber.lab,ccl2.cyber.lab,ccl3.cyber.lab}"
IFS=',' read -r -a BENCH_SYSTEMS <<< "${BENCH_SYSTEMS_CSV}"

SLOWDOWN_TYPE_RAW="${SLOWDOWN_TYPE:-cpu}"
SLOWDOWN_VALUE="${SLOWDOWN_VALUE:-4}"
DEFAULT_CORES="${DEFAULT_CORES:-8}"
DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT:-10000}"
DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS:-0ms}"
DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB:-65536}"

CASS_CONNECTIONS="${CASS_CONNECTIONS:-8}"
CASS_KEYSPACE="${CASS_KEYSPACE:-ycsb}"
CASS_TABLE="${CASS_TABLE:-usertable}"
CASS_LOCAL_DC="${CASS_LOCAL_DC:-datacenter1}"
CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"
SILENCE="${SILENCE:-true}"
WORKLOAD_TARGET_WAS_SET="${WORKLOAD_TARGET+x}"
WORKLOAD_TARGET="${WORKLOAD_TARGET:-32000}"
THREAD_COUNT="${THREAD_COUNT:-64}"
RUN_DURATION="${RUN_DURATION:-30}"
SINGLE_TRIALS="${SINGLE_TRIALS:-1}"
RECORD_COUNT="${RECORD_COUNT:-200000}"
OPERATION_COUNT="${OPERATION_COUNT:-2000000}"
WARMUP_QUERIES="${WARMUP_QUERIES:-5}"
WARMUP_SLEEP_SEC="${WARMUP_SLEEP_SEC:-2}"
CPU_FLAKY_MODE="${CPU_FLAKY_MODE:-false}"
CPU_HEALTHY_CORES="${CPU_HEALTHY_CORES:-$DEFAULT_CORES}"
CPU_UNHEALTHY_CORES="${CPU_UNHEALTHY_CORES:-1}"
CPU_HEALTHY_INTERVAL_SEC="${CPU_HEALTHY_INTERVAL_SEC:-30}"
CPU_UNHEALTHY_INTERVAL_SEC="${CPU_UNHEALTHY_INTERVAL_SEC:-10}"

RUN_TAG="${RUN_TAG:-$(date +%Y%m%d_%H%M%S)_$RANDOM}"
SLOWDOWN_NODES_CSV="${SLOWDOWN_NODES_CSV:-}"
CLEAN_REMOTE_RESULTS="${CLEAN_REMOTE_RESULTS:-false}"

THROUGHPUT_MODE="${THROUGHPUT_MODE:-auto}" # auto|single|search
LATENCY_TARGET_MS="${LATENCY_TARGET_MS:-25}"
LATENCY_TARGET_US="$(awk -v ms="${LATENCY_TARGET_MS}" 'BEGIN {printf "%.4f", ms * 1000.0}')"
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
TARGET_CLOSE_TOLERANCE_US="${TARGET_CLOSE_TOLERANCE_US:-250}"
REFINE_MAX_EVALS="${REFINE_MAX_EVALS:-4}"
VALIDATION_BACKOFF_STEPS="${VALIDATION_BACKOFF_STEPS:-6}"
POST_VALIDATE_CLIMB_EVALS="${POST_VALIDATE_CLIMB_EVALS:-6}"

EVAL_LAST_AVG_THR="0"
EVAL_LAST_AVG_LAT_US="0"
EVAL_LAST_MEAN_LAT_US="0"
EVAL_LAST_MEDIAN_LAT_US="0"
EVAL_LAST_P99_LAT_US="0"
EVAL_LAST_PASSED="false"
EVAL_LAST_STATUS="ok"

SEARCH_RESULT_BEST=0
SEARCH_RESULT_LOW_PASS=0
SEARCH_RESULT_HIGH_FAIL=0
SEARCH_RESULT_BEST_LAT_US=0
SEARCH_RESULT_LOW_PASS_LAT_US=0
SEARCH_RESULT_HIGH_FAIL_LAT_US=0
CURRENT_EVAL_INDEX=0

SLOWDOWN_NODES=()
if [[ -n "${SLOWDOWN_NODES_CSV}" ]]; then
    IFS=',' read -r -a SLOWDOWN_NODES <<< "${SLOWDOWN_NODES_CSV}"
fi

declare -a CPU_FLAKY_PIDS=()
declare -a CPU_FLAKY_HOSTS=()
declare -a CPU_FLAKY_CONTAINERS=()

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

die() {
    log "ERROR: $*"
    exit 1
}

is_positive_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

float_le() {
    local a="$1"
    local b="$2"
    awk -v a="$a" -v b="$b" 'BEGIN {exit !(a <= b)}'
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

latency_is_very_close() {
    local lat_us="$1"
    awk -v lat="$lat_us" -v target="$LATENCY_TARGET_US" -v tol="$TARGET_CLOSE_TOLERANCE_US" '
        BEGIN {
            gap = target - lat
            if (gap < 0) gap = -gap
            exit !(gap <= tol)
        }'
}

latency_is_far_below_target() {
    local lat_us="$1"
    awk -v lat="$lat_us" -v target="$LATENCY_TARGET_US" -v tol="$TARGET_CLOSE_TOLERANCE_US" 'BEGIN {exit !(lat < (target - tol))}'
}

round_up_to_granularity() {
    local val="$1"
    echo $((((val + THROUGHPUT_GRANULARITY - 1) / THROUGHPUT_GRANULARITY) * THROUGHPUT_GRANULARITY))
}

round_down_to_granularity() {
    local val="$1"
    echo $(((val / THROUGHPUT_GRANULARITY) * THROUGHPUT_GRANULARITY))
}

clamp_throughput() {
    local val="$1"
    local low="$2"
    local high="$3"
    if (( val < low )); then
        val="$low"
    fi
    if (( val > high )); then
        val="$high"
    fi
    echo "$val"
}

resolve_throughput_mode() {
    case "$THROUGHPUT_MODE" in
        single|search)
            echo "$THROUGHPUT_MODE"
            ;;
        auto)
            if [[ -n "${WORKLOAD_TARGET_WAS_SET}" ]]; then
                echo "single"
            else
                echo "search"
            fi
            ;;
        *)
            die "Unsupported THROUGHPUT_MODE='${THROUGHPUT_MODE}' (expected auto|single|search)"
            ;;
    esac
}

normalize_slowdown_type() {
    case "$1" in
        cpu|memory|network|latency) echo "$1" ;;
        network_bandwidth) echo "network" ;;
        network_latency) echo "latency" ;;
        *) return 1 ;;
    esac
}

SLOWDOWN_TYPE="$(normalize_slowdown_type "${SLOWDOWN_TYPE_RAW}")" || die "Unsupported SLOWDOWN_TYPE='${SLOWDOWN_TYPE_RAW}'"

is_slowdown_node() {
    local host="$1"
    local node

    if [[ "${#SLOWDOWN_NODES[@]}" -eq 0 ]]; then
        return 0
    fi

    for node in "${SLOWDOWN_NODES[@]}"; do
        if [[ "$node" == "$host" ]]; then
            return 0
        fi
    done

    return 1
}

make_systems_list() {
    local systems=()
    local host
    for host in "${BENCH_SYSTEMS[@]}"; do
        systems+=("${host}:9042")
    done
    local IFS=,
    echo "${systems[*]}"
}

make_container_names() {
    local names=()
    local idx
    for idx in "${!BENCH_SYSTEMS[@]}"; do
        names+=("cassandra-node-$((idx + 1))")
    done
    local IFS=,
    echo "${names[*]}"
}

default_interface_for_host() {
    local host="$1"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "ip -o -4 route show to default | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p' | head -n1" 2>/dev/null
}

ifb_device_for_host() {
    local host="$1"
    local host_tag

    host_tag="$(printf '%s' "${host%%.*}" | tr -cd '[:alnum:]' | tr '[:upper:]' '[:lower:]')"
    if [[ -z "$host_tag" ]]; then
        host_tag="cass"
    fi

    echo "ifb${host_tag:0:11}"
}

latency_is_zero() {
    local val="$1"
    [[ "$val" =~ ^0+([a-zA-Z]+)?$ ]]
}

apply_core_restraints() {
    local host="$1"
    local num_cores="$2"
    local container_name="$3"
    local docker_cores="0-$((num_cores - 1))"

    log "Applying CPU limit on $host ($container_name): $docker_cores"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker update --cpuset-cpus='${docker_cores}' '${container_name}'" >/dev/null
}

remove_core_restraints() {
    local host="$1"
    local container_name="$2"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker update --cpuset-cpus='' '${container_name}'" >/dev/null 2>&1 || true
}

apply_bandwidth_restraints() {
    local host="$1"
    local target_mbit="$2"
    local container_name="$3"
    local iface
    local ifb_dev

    iface="$(default_interface_for_host "$host")"
    [[ -n "$iface" ]] || die "Could not determine default interface for $host"
    ifb_dev="$(ifb_device_for_host "$host")"

    log "Applying bandwidth limit on $host ($container_name): ${target_mbit}mbit egress+ingress via $iface/$ifb_dev"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" \
        "IFACE='$iface' TARGET_MBIT='$target_mbit' IFB_DEV='$ifb_dev' bash -s" <<'EOREMOTE' >/dev/null
set -euo pipefail

sudo modprobe ifb >/dev/null 2>&1 || true

if ! ip link show "$IFB_DEV" >/dev/null 2>&1; then
    sudo ip link add "$IFB_DEV" type ifb
fi
sudo ip link set dev "$IFB_DEV" up

# Egress shaping.
# Use delete+add for better compatibility across tc/qdisc versions.
sudo tc qdisc del dev "$IFACE" root >/dev/null 2>&1 || true
sudo tc qdisc add dev "$IFACE" root handle 1: htb default 10 r2q 1000
sudo tc class add dev "$IFACE" parent 1: classid 1:10 htb rate "${TARGET_MBIT}mbit" ceil "${TARGET_MBIT}mbit" quantum 1514

# Ingress shaping via IFB redirection.
sudo tc qdisc del dev "$IFACE" ingress >/dev/null 2>&1 || true
sudo tc qdisc add dev "$IFACE" ingress
sudo tc filter add dev "$IFACE" parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev "$IFB_DEV"
sudo tc filter add dev "$IFACE" parent ffff: protocol ipv6 u32 match u32 0 0 action mirred egress redirect dev "$IFB_DEV" >/dev/null 2>&1 || true
sudo tc qdisc del dev "$IFB_DEV" root >/dev/null 2>&1 || true
sudo tc qdisc add dev "$IFB_DEV" root handle 2: htb default 10 r2q 1000
sudo tc class add dev "$IFB_DEV" parent 2: classid 2:10 htb rate "${TARGET_MBIT}mbit" ceil "${TARGET_MBIT}mbit" quantum 1514
EOREMOTE
}

remove_bandwidth_restraints() {
    local host="$1"
    local container_name="$2"
    local iface
    local ifb_dev

    iface="$(default_interface_for_host "$host")"
    [[ -n "$iface" ]] || return
    ifb_dev="$(ifb_device_for_host "$host")"

    ssh ${SSH_OPTS} "$BENCH_USER@$host" \
        "IFACE='$iface' IFB_DEV='$ifb_dev' bash -s" <<'EOREMOTE' >/dev/null 2>&1 || true
sudo tc qdisc del dev "$IFACE" ingress >/dev/null 2>&1 || true
sudo tc qdisc del dev "$IFACE" root >/dev/null 2>&1 || true
sudo tc qdisc del dev "$IFB_DEV" root >/dev/null 2>&1 || true
sudo ip link set dev "$IFB_DEV" down >/dev/null 2>&1 || true
sudo ip link del "$IFB_DEV" type ifb >/dev/null 2>&1 || true
EOREMOTE
}

apply_latency_restraints() {
    local host="$1"
    local target_latency="$2"
    local container_name="$3"
    local iface

    iface="$(default_interface_for_host "$host")"
    [[ -n "$iface" ]] || die "Could not determine default interface for $host"

    if latency_is_zero "$target_latency"; then
        ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc del dev '$iface' root >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
        return
    fi

    log "Applying latency limit on $host ($container_name): $target_latency via $iface"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc replace dev '$iface' root netem delay ${target_latency}" >/dev/null
}

remove_latency_restraints() {
    local host="$1"
    local container_name="$2"
    local iface

    iface="$(default_interface_for_host "$host")"
    [[ -n "$iface" ]] || return
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc del dev '$iface' root >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}

apply_memory_restraints() {
    local host="$1"
    local target_available_mb="$2"
    local host_tag="${host%%.*}"
    local memhog_file="/dev/shm/cass_memhog_${host_tag}.bin"
    local hog_mb=$((DEFAULT_MEMORY_MB - target_available_mb))

    if (( hog_mb < 0 )); then
        hog_mb=0
    fi

    if (( hog_mb == 0 )); then
        ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f '$memhog_file'" >/dev/null 2>&1 || true
        return
    fi

    log "Applying memory pressure on $host: reserving ${hog_mb}MB (target ${target_available_mb}MB free)"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f '$memhog_file' && dd if=/dev/zero of='$memhog_file' bs=1M count=${hog_mb} status=none" >/dev/null
}

remove_memory_restraints() {
    local host="$1"
    local container_name="$2"
    local host_tag="${host%%.*}"
    local memhog_file="/dev/shm/cass_memhog_${host_tag}.bin"

    ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f '$memhog_file'" >/dev/null 2>&1 || true
}

apply_resource_restraints() {
    local host="$1"
    local value="$2"
    local container_name="$3"

    case "$SLOWDOWN_TYPE" in
        cpu) apply_core_restraints "$host" "$value" "$container_name" ;;
        network) apply_bandwidth_restraints "$host" "$value" "$container_name" ;;
        latency) apply_latency_restraints "$host" "$value" "$container_name" ;;
        memory) apply_memory_restraints "$host" "$value" "$container_name" ;;
        *) die "Unsupported normalized SLOWDOWN_TYPE '$SLOWDOWN_TYPE'" ;;
    esac
}

remove_resource_restraints() {
    local host="$1"
    local container_name="$2"

    case "$SLOWDOWN_TYPE" in
        cpu) remove_core_restraints "$host" "$container_name" ;;
        network) remove_bandwidth_restraints "$host" "$container_name" ;;
        latency) remove_latency_restraints "$host" "$container_name" ;;
        memory) remove_memory_restraints "$host" "$container_name" ;;
        *) true ;;
    esac
}

start_cpu_flaky_loops() {
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers=()
    local idx
    local host
    local container

    IFS=',' read -r -a containers <<< "$(make_container_names)"
    CPU_FLAKY_PIDS=()
    CPU_FLAKY_HOSTS=()
    CPU_FLAKY_CONTAINERS=()

    log "Starting CPU flaky toggles: healthy=${CPU_HEALTHY_CORES}c for ${CPU_HEALTHY_INTERVAL_SEC}s, unhealthy=${CPU_UNHEALTHY_CORES}c for ${CPU_UNHEALTHY_INTERVAL_SEC}s"
    for idx in "${!systems[@]}"; do
        host="${systems[$idx]}"
        container="${containers[$idx]}"
        if ! is_slowdown_node "$host"; then
            continue
        fi
        CPU_FLAKY_HOSTS+=("$host")
        CPU_FLAKY_CONTAINERS+=("$container")
        (
            while true; do
                sleep "$CPU_HEALTHY_INTERVAL_SEC"
                apply_core_restraints "$host" "$CPU_UNHEALTHY_CORES" "$container" || true
                sleep "$CPU_UNHEALTHY_INTERVAL_SEC"
                apply_core_restraints "$host" "$CPU_HEALTHY_CORES" "$container" || true
            done
        ) &
        CPU_FLAKY_PIDS+=("$!")
    done
}

stop_cpu_flaky_loops() {
    local idx
    for pid in "${CPU_FLAKY_PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
    for pid in "${CPU_FLAKY_PIDS[@]:-}"; do
        wait "$pid" >/dev/null 2>&1 || true
    done
    for idx in "${!CPU_FLAKY_HOSTS[@]}"; do
        apply_core_restraints "${CPU_FLAKY_HOSTS[$idx]}" "$CPU_HEALTHY_CORES" "${CPU_FLAKY_CONTAINERS[$idx]}" || true
    done
    CPU_FLAKY_PIDS=()
    CPU_FLAKY_HOSTS=()
    CPU_FLAKY_CONTAINERS=()
}

default_step_value() {
    case "$SLOWDOWN_TYPE" in
        cpu) echo "$DEFAULT_CORES" ;;
        network) echo "$DEFAULT_BANDWIDTH_MBIT" ;;
        latency) echo "$DEFAULT_LATENCY_MS" ;;
        memory) echo "$DEFAULT_MEMORY_MB" ;;
        *) die "Unsupported normalized SLOWDOWN_TYPE '$SLOWDOWN_TYPE'" ;;
    esac
}

wait_for_cluster_ready() {
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    local idx

    IFS=',' read -r -a containers <<< "$(make_container_names)"

    for idx in "${!systems[@]}"; do
        local host="${systems[$idx]}"
        local container="${containers[$idx]}"
        local attempt

        for attempt in $(seq 1 90); do
            if ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker exec -i '$container' cqlsh -e \"SELECT now() FROM system.local\" >/dev/null 2>&1"; then
                break
            fi
            if [[ "$attempt" -eq 90 ]]; then
                die "CQL not ready on ${host} (${container}) after applying restraints"
            fi
            sleep 2
        done
    done
}

warmup_cluster() {
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    local idx

    IFS=',' read -r -a containers <<< "$(make_container_names)"

    log "Warming up cluster with ${WARMUP_QUERIES} lightweight CQL probe(s) per node"
    for idx in "${!systems[@]}"; do
        local host="${systems[$idx]}"
        local container="${containers[$idx]}"

        ssh ${SSH_OPTS} "$BENCH_USER@$host" "
            set -euo pipefail
            for i in \$(seq 1 ${WARMUP_QUERIES}); do
                docker exec -i '${container}' cqlsh -e \"SELECT now() FROM system.local\" >/dev/null 2>&1
            done
        " >/dev/null
    done

    if [[ "${WARMUP_SLEEP_SEC}" -gt 0 ]]; then
        sleep "${WARMUP_SLEEP_SEC}"
    fi
}

cleanup() {
    set +e
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    local idx

    if [[ "$CPU_FLAKY_MODE" == "true" ]]; then
        stop_cpu_flaky_loops
    fi

    IFS=',' read -r -a containers <<< "$(make_container_names)"
    for idx in "${!systems[@]}"; do
        remove_resource_restraints "${systems[$idx]}" "${containers[$idx]}"
    done
    set -e
}

run_benchmark() {
    local step_value="$1"
    local target_throughput="$2"
    local run_duration="$3"
    local phase="${4:-run}"
    local trial="${5:-1}"
    local attempt="${6:-1}"
    local run_tag="${RUN_TAG}_${phase}_${target_throughput}_t${trial}_a${attempt}_$(date +%s%N)"
    local results_dir="${RESULTS_DIR_BASE}/${run_tag}"
    local prefix="cassandra_${run_tag}"
    local systems_list

    systems_list="$(make_systems_list)"

    log "Benchmark run: phase=${phase} step=${step_value} target=${target_throughput} duration=${run_duration}s trial=${trial} attempt=${attempt}" >&2

    ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" \
        "CASS_BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' CASS_SYSTEMS_LIST='$systems_list' CASS_RESULTS_DIR='$results_dir' CASS_PREFIX='$prefix' CASS_KEYSPACE='$CASS_KEYSPACE' CASS_TABLE='$CASS_TABLE' CASS_LOCAL_DC='$CASS_LOCAL_DC' CASS_CLIENT_TYPE='$CLIENT_TYPE' CASS_SILENCE='$SILENCE' CASS_WORKLOAD_TARGET='$target_throughput' CASS_THREAD_COUNT='$THREAD_COUNT' CASS_RUN_DURATION='$run_duration' CASS_RECORD_COUNT='$RECORD_COUNT' CASS_OPERATION_COUNT='$OPERATION_COUNT' CASS_CONNECTIONS='$CASS_CONNECTIONS' CASS_PHASE='$phase' CASS_TRIAL='$trial' bash -s" <<'EOREMOTE'
set -euo pipefail

mkdir -p "$CASS_RESULTS_DIR"
rm -rf "$CASS_RESULTS_DIR"/* 2>/dev/null || true

WORKLOAD_FILE="/tmp/workload_${CASS_PREFIX}.in"
cat > "$WORKLOAD_FILE" <<EOW
workload=core
recordcount=$CASS_RECORD_COUNT
operationcount=$CASS_OPERATION_COUNT
cassandra.cluster=$CASS_SYSTEMS_LIST
cassandra.hosts=$CASS_SYSTEMS_LIST
cassandra.port=9042
cassandra.keyspace=$CASS_KEYSPACE
cassandra.table=$CASS_TABLE
cassandra.localdatacenter=$CASS_LOCAL_DC
cassandra.readconsistencylevel=ONE
cassandra.writeconsistencylevel=QUORUM
cassandra.connections=$CASS_CONNECTIONS
table=$CASS_TABLE
threadcount=$CASS_THREAD_COUNT
target=$CASS_WORKLOAD_TARGET
duration=$CASS_RUN_DURATION
measurement.type=raw
clienttype=$CASS_CLIENT_TYPE
silence=$CASS_SILENCE
csvfilename=$CASS_PREFIX
EOW

trap 'rm -f "$WORKLOAD_FILE"' EXIT

LOAD_LOG="${CASS_RESULTS_DIR}/${CASS_PREFIX}_load.log"
RUN_LOG="${CASS_RESULTS_DIR}/${CASS_PREFIX}_run.log"

pushd "$CASS_RESULTS_DIR" >/dev/null
if ! "$CASS_BENCH_DIR_ABSOLUTE/bin/unh-bench" load cassandra -P "$WORKLOAD_FILE" >"$LOAD_LOG" 2>&1; then
    echo "Load phase failed. See $LOAD_LOG" >&2
    tail -n 50 "$LOAD_LOG" >&2 || true
    exit 1
fi

rm -f "${CASS_PREFIX}_primary_"*.csv 2>/dev/null || true

if ! "$CASS_BENCH_DIR_ABSOLUTE/bin/unh-bench" run cassandra -P "$WORKLOAD_FILE" >"$RUN_LOG" 2>&1; then
    echo "Run phase failed. See $RUN_LOG" >&2
    tail -n 50 "$RUN_LOG" >&2 || true
    exit 1
fi
popd >/dev/null
EOREMOTE

    local result_pair
    result_pair="$(ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" "CASS_BENCH_DIR_ABSOLUTE='$BENCH_DIR_ABSOLUTE' CASS_RESULTS_DIR='${results_dir}' CASS_PREFIX='${prefix}' CASS_WORKLOAD_TARGET='$target_throughput' CASS_PHASE='$phase' CASS_TRIAL='$trial' bash -s" <<'EOREMOTE'
set -euo pipefail

shopt -s nullglob
raw_files=( "${CASS_RESULTS_DIR}/${CASS_PREFIX}_primary_"*.csv )
if (( ${#raw_files[@]} == 0 )); then
    echo "No raw run CSV files found in ${CASS_RESULTS_DIR} for ${CASS_PREFIX}" >&2
    exit 1
fi

read -r total_ops error_ops <<< "$(awk -F, 'FNR > 1 {tot += 1; if ($1 ~ /_ERROR$/) err += 1} END {printf "%d %d", tot + 0, err + 0}' "${raw_files[@]}")"
if (( total_ops == 0 )); then
    echo "Raw run CSV files contain zero operations" >&2
    exit 1
fi
if (( error_ops > 0 )); then
    echo "Found ${error_ops}/${total_ops} *_ERROR rows" >&2
    awk -F, 'FNR > 1 && $1 ~ /_ERROR$/ {print FILENAME ":" FNR ":" $0; n += 1; if (n == 5) exit}' "${raw_files[@]}" >&2
    exit 1
fi

pushd "$CASS_RESULTS_DIR" >/dev/null
python3 "$CASS_BENCH_DIR_ABSOLUTE/tool/analysis/summary_stats.py" -p "$CASS_PREFIX" -f "$CASS_RESULTS_DIR/" -w 1 >/dev/null

sumstat_files=( "${CASS_PREFIX}_"*_sumstat.csv )
if (( ${#sumstat_files[@]} == 0 )); then
    echo "No sumstat CSV produced for prefix ${CASS_PREFIX}" >&2
    exit 1
fi

summary_csv="${sumstat_files[0]}"
cp "$summary_csv" "${CASS_PREFIX}.csv"

avg_throughput=$(awk -F, 'NR > 1 && $2 > 0 {sum += $2; n += 1} END {if (n > 0) printf "%.4f", sum / n; else print "N/A"}' "${CASS_PREFIX}.csv")
avg_latency=$(awk -F, 'NR > 1 && $2 > 0 && $3 > 0 {weighted += ($2 * $3); ops += $2} END {if (ops > 0) printf "%.4f", weighted / ops; else print "N/A"}' "${CASS_PREFIX}.csv")

awk -F, -v phase="$CASS_PHASE" -v target="$CASS_WORKLOAD_TARGET" -v trial="$CASS_TRIAL" '
    NR > 1 && $1 ~ /^[0-9]+$/ {
        printf "TRIAL_SERIES:%s,%s,%s,%s,%.4f,%.4f,%.4f\n", phase, target, trial, $1, ($2 + 0), ($3 + 0), ($4 + 0) > "/dev/stderr"
    }' "${CASS_PREFIX}.csv"

latency_triplet="$(python3 - <<'PY'
import csv
import glob
import math
import os
import statistics
import sys

results_dir = os.environ["CASS_RESULTS_DIR"]
prefix = os.environ["CASS_PREFIX"]
latencies = []

for path in glob.glob(os.path.join(results_dir, f"{prefix}_primary_*.csv")):
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row or row[0] == "Operation":
                continue
            if len(row) < 4:
                continue
            op = row[0]
            if op.endswith("_ERROR"):
                continue
            try:
                start = int(row[2])
                end = int(row[3])
            except ValueError:
                continue
            if end >= start:
                latencies.append(end - start)

if not latencies:
    print("N/A,N/A,N/A")
    sys.exit(0)

latencies.sort()
n = len(latencies)
p99 = float(latencies[max(0, math.ceil(0.99 * n) - 1)])
mean_lat = statistics.mean(latencies)
median_lat = statistics.median(latencies)
print(f"{mean_lat:.4f},{median_lat:.4f},{p99:.4f}")
PY
)"
IFS=',' read -r mean_lat median_lat p99_lat <<< "$latency_triplet"
popd >/dev/null

echo "${avg_throughput},${median_lat},${mean_lat},${median_lat},${p99_lat}"
EOREMOTE
)"

    if [[ "${CLEAN_REMOTE_RESULTS}" == "true" ]]; then
        ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" "rm -rf '${results_dir}'" >/dev/null 2>&1 || true
    fi

    if [[ -z "$result_pair" ]]; then
        die "Failed to parse result pair from benchmark run"
    fi

    echo "$result_pair"
}

run_trial_once() {
    local step_value="$1"
    local target_throughput="$2"
    local phase="$3"
    local run_duration="$4"
    local trial="$5"
    local attempt="$6"
    local pair

    pair="$(run_benchmark "$step_value" "$target_throughput" "$run_duration" "$phase" "$trial" "$attempt")"
    local mean_lat
    local median_lat
    local p99_lat
    IFS=',' read -r thr lat mean_lat median_lat p99_lat <<< "$pair"
    if [[ -z "${thr:-}" || -z "${lat:-}" || -z "${mean_lat:-}" || -z "${median_lat:-}" || -z "${p99_lat:-}" ]]; then
        return 1
    fi
    if [[ "$thr" == "N/A" || "$lat" == "N/A" || "$mean_lat" == "N/A" || "$median_lat" == "N/A" || "$p99_lat" == "N/A" ]]; then
        return 1
    fi
    echo "$pair"
}

evaluate_target() {
    local step_value="$1"
    local target_throughput="$2"
    local phase="$3"
    local trial_count="$4"
    local run_duration="$5"
    local trial
    local attempt
    local pair
    local thr
    local lat
    local status="ok"
    local passed="false"
    local avg_thr="0"
    local median_lat="999999999"
    local -a trial_thrs=()
    local -a trial_lats=()
    local -a trial_mean_lats=()
    local -a trial_median_lats=()
    local -a trial_p99_lats=()
    local mean_lat="0"
    local median_lat_detail="0"
    local p99_lat="0"

    ((CURRENT_EVAL_INDEX += 1))
    log "Eval ${CURRENT_EVAL_INDEX}: phase=${phase} target=${target_throughput} trials=${trial_count} duration=${run_duration}s"

    for trial in $(seq 1 "$trial_count"); do
        pair=""
        for attempt in $(seq 1 "$MAX_TRIAL_ATTEMPTS"); do
            if pair="$(run_trial_once "$step_value" "$target_throughput" "$phase" "$run_duration" "$trial" "$attempt")"; then
                break
            fi
            if [[ "$attempt" -lt "$MAX_TRIAL_ATTEMPTS" ]]; then
                log "Retrying phase=${phase} target=${target_throughput} trial=${trial}"
            fi
        done

        if [[ -z "$pair" ]]; then
            status="error"
            break
        fi

        IFS=',' read -r thr lat mean_lat median_lat_detail p99_lat <<< "$pair"
        trial_thrs+=("$thr")
        trial_lats+=("$lat")
        trial_mean_lats+=("$mean_lat")
        trial_median_lats+=("$median_lat_detail")
        trial_p99_lats+=("$p99_lat")
        echo "TRIAL_RESULT:${phase},${target_throughput},${trial},${thr},${lat},${mean_lat},${median_lat_detail},${p99_lat}"
    done

    if [[ "$status" == "ok" ]] && [[ "${#trial_thrs[@]}" -gt 0 ]]; then
        avg_thr="$(mean_of "${trial_thrs[@]}")"
        median_lat="$(median_of "${trial_lats[@]}")"
        if float_le "$median_lat" "$LATENCY_TARGET_US"; then
            passed="true"
        fi
    fi

    EVAL_LAST_AVG_THR="$avg_thr"
    EVAL_LAST_AVG_LAT_US="$median_lat"
    EVAL_LAST_MEAN_LAT_US="$(mean_of "${trial_mean_lats[@]}")"
    EVAL_LAST_MEDIAN_LAT_US="$(median_of "${trial_median_lats[@]}")"
    EVAL_LAST_P99_LAT_US="$(mean_of "${trial_p99_lats[@]}")"
    EVAL_LAST_PASSED="$passed"
    EVAL_LAST_STATUS="$status"

    log "Eval ${CURRENT_EVAL_INDEX} result: target=${target_throughput} avg_thr=${avg_thr} median_lat_us=${median_lat} mean_lat_us=${EVAL_LAST_MEAN_LAT_US} p99_lat_us=${EVAL_LAST_P99_LAT_US} pass=${passed} status=${status}"
}

search_highest_pass() {
    local step_value="$1"
    local search_low="$2"
    local search_high="$3"
    local initial_probe="$4"
    local eval_count=0
    local low
    local high
    local mid
    local best=0
    local best_lat_us=0
    local low_pass=0
    local low_pass_lat_us=0
    local high_fail=0
    local high_fail_lat_us=0
    low="$search_low"
    high="$(round_down_to_granularity "$search_high")"
    low="$(clamp_throughput "$low" "$search_low" "$search_high")"
    high="$(clamp_throughput "$high" "$search_low" "$search_high")"

    initial_probe="$(round_down_to_granularity "$initial_probe")"
    initial_probe="$(clamp_throughput "$initial_probe" "$low" "$high")"
    if (( initial_probe >= low && initial_probe <= high )); then
        evaluate_target "$step_value" "$initial_probe" "find" "$FIND_TRIALS" "$FIND_RUN_DURATION"
        ((eval_count += 1))
        if [[ "$EVAL_LAST_PASSED" == "true" ]] && [[ "$EVAL_LAST_STATUS" == "ok" ]]; then
            best="$initial_probe"
            best_lat_us="$EVAL_LAST_AVG_LAT_US"
            low_pass="$initial_probe"
            low_pass_lat_us="$EVAL_LAST_AVG_LAT_US"
            low=$((initial_probe + THROUGHPUT_GRANULARITY))
        else
            high_fail="$initial_probe"
            high_fail_lat_us="$EVAL_LAST_AVG_LAT_US"
            high=$((initial_probe - THROUGHPUT_GRANULARITY))
        fi
    fi

    while (( low <= high )) && (( eval_count < MAX_EVAL_POINTS )); do
        mid=$(((low + high) / 2))
        mid="$(round_down_to_granularity "$mid")"
        mid="$(clamp_throughput "$mid" "$low" "$high")"

        evaluate_target "$step_value" "$mid" "find" "$FIND_TRIALS" "$FIND_RUN_DURATION"
        ((eval_count += 1))

        if [[ "$EVAL_LAST_PASSED" == "true" ]] && [[ "$EVAL_LAST_STATUS" == "ok" ]]; then
            best="$mid"
            best_lat_us="$EVAL_LAST_AVG_LAT_US"
            low_pass="$mid"
            low_pass_lat_us="$EVAL_LAST_AVG_LAT_US"
            low=$((mid + THROUGHPUT_GRANULARITY))
        else
            high_fail="$mid"
            high_fail_lat_us="$EVAL_LAST_AVG_LAT_US"
            high=$((mid - THROUGHPUT_GRANULARITY))
        fi
    done

    SEARCH_RESULT_BEST="$best"
    SEARCH_RESULT_BEST_LAT_US="$best_lat_us"
    SEARCH_RESULT_LOW_PASS="$low_pass"
    SEARCH_RESULT_LOW_PASS_LAT_US="$low_pass_lat_us"
    SEARCH_RESULT_HIGH_FAIL="$high_fail"
    SEARCH_RESULT_HIGH_FAIL_LAT_US="$high_fail_lat_us"
}

refine_toward_latency_target() {
    local step_value="$1"
    local low_pass="$2"
    local high_fail="$3"
    local low
    local high
    local mid
    local eval_count=0
    local near_target="false"

    low=$((low_pass + THROUGHPUT_GRANULARITY))
    if (( high_fail > 0 )); then
        high=$((high_fail - THROUGHPUT_GRANULARITY))
    else
        high="$MAX_THROUGHPUT"
    fi

    low="$(round_up_to_granularity "$low")"
    high="$(round_down_to_granularity "$high")"
    low="$(clamp_throughput "$low" "$MIN_THROUGHPUT" "$MAX_THROUGHPUT")"
    high="$(clamp_throughput "$high" "$MIN_THROUGHPUT" "$MAX_THROUGHPUT")"

    while (( low <= high )) && (( eval_count < REFINE_MAX_EVALS )); do
        mid=$(((low + high) / 2))
        mid="$(round_down_to_granularity "$mid")"
        mid="$(clamp_throughput "$mid" "$low" "$high")"

        evaluate_target "$step_value" "$mid" "refine" "$FIND_TRIALS" "$FIND_RUN_DURATION"
        ((eval_count += 1))

        if [[ "$EVAL_LAST_PASSED" == "true" ]] && [[ "$EVAL_LAST_STATUS" == "ok" ]]; then
            SEARCH_RESULT_BEST="$mid"
            SEARCH_RESULT_BEST_LAT_US="$EVAL_LAST_AVG_LAT_US"
            SEARCH_RESULT_LOW_PASS="$mid"
            SEARCH_RESULT_LOW_PASS_LAT_US="$EVAL_LAST_AVG_LAT_US"
            low=$((mid + THROUGHPUT_GRANULARITY))
            if latency_is_very_close "$EVAL_LAST_AVG_LAT_US"; then
                near_target="true"
            else
                near_target="false"
            fi
            if [[ "$near_target" == "true" ]]; then
                break
            fi
        else
            SEARCH_RESULT_HIGH_FAIL="$mid"
            SEARCH_RESULT_HIGH_FAIL_LAT_US="$EVAL_LAST_AVG_LAT_US"
            high=$((mid - THROUGHPUT_GRANULARITY))
        fi
    done
}

run_single_target() {
    local step_value="$1"
    evaluate_target "$step_value" "$WORKLOAD_TARGET" "single" "$SINGLE_TRIALS" "$RUN_DURATION"
    if [[ "$EVAL_LAST_STATUS" != "ok" ]]; then
        die "Single-target benchmark failed at target=${WORKLOAD_TARGET}"
    fi
    log "Single-target complete: slowdown_type=${SLOWDOWN_TYPE} step=${step_value} target=${WORKLOAD_TARGET} observed_throughput=${EVAL_LAST_AVG_THR} observed_latency_us=${EVAL_LAST_AVG_LAT_US}"
    echo "RESULTS:${EVAL_LAST_AVG_THR},${EVAL_LAST_AVG_LAT_US}"
    echo "RESULTS_EXT:${EVAL_LAST_AVG_THR},${EVAL_LAST_AVG_LAT_US},${EVAL_LAST_MEAN_LAT_US},${EVAL_LAST_MEDIAN_LAT_US},${EVAL_LAST_P99_LAT_US}"
}

validation_passes() {
    [[ "$EVAL_LAST_STATUS" == "ok" ]] && [[ "$EVAL_LAST_PASSED" == "true" ]]
}

run_search_mode() {
    local step_value="$1"
    local search_low="$MIN_THROUGHPUT"
    local search_high="$MAX_THROUGHPUT"
    local initial_probe="$INITIAL_THROUGHPUT"
    local candidate=0
    local final_thr="0"
    local final_lat="999999999"
    local validated="false"

    log "Max-throughput binary search: step=${step_value} latency_target_ms=${LATENCY_TARGET_MS} (latency_target_us=${LATENCY_TARGET_US}) bounds=[${search_low},${search_high}]"
    search_highest_pass "$step_value" "$search_low" "$search_high" "$initial_probe"
    candidate="$SEARCH_RESULT_BEST"

    if (( candidate > 0 )); then
        if latency_is_far_below_target "$SEARCH_RESULT_BEST_LAT_US"; then
            log "Refining search upward: best_median_latency_us=${SEARCH_RESULT_BEST_LAT_US} is farther than ${TARGET_CLOSE_TOLERANCE_US}us from target"
            refine_toward_latency_target "$step_value" "$SEARCH_RESULT_LOW_PASS" "$SEARCH_RESULT_HIGH_FAIL"
            candidate="$SEARCH_RESULT_BEST"
        fi

        log "Validating candidate throughput=${candidate} with ${VALIDATION_TRIALS} trial(s)"
        evaluate_target "$step_value" "$candidate" "validate" "$VALIDATION_TRIALS" "$VALIDATION_RUN_DURATION"
        final_thr="$EVAL_LAST_AVG_THR"
        final_lat="$EVAL_LAST_AVG_LAT_US"
        if validation_passes; then
            validated="true"
        else
            local backoff_candidate=$((candidate - THROUGHPUT_GRANULARITY))
            local backoff_eval_count=0
            log "Candidate failed validation, probing lower targets (max ${VALIDATION_BACKOFF_STEPS})"
            while (( backoff_candidate >= MIN_THROUGHPUT )) && (( backoff_eval_count < VALIDATION_BACKOFF_STEPS )); do
                evaluate_target "$step_value" "$backoff_candidate" "validate_backoff" "$VALIDATION_TRIALS" "$VALIDATION_RUN_DURATION"
                ((backoff_eval_count += 1))
                if validation_passes; then
                    candidate="$backoff_candidate"
                    final_thr="$EVAL_LAST_AVG_THR"
                    final_lat="$EVAL_LAST_AVG_LAT_US"
                    validated="true"
                    break
                fi
                backoff_candidate=$((backoff_candidate - THROUGHPUT_GRANULARITY))
            done
        fi

        if [[ "$validated" == "true" ]] && latency_is_far_below_target "$final_lat"; then
            local climb_candidate=$((candidate + THROUGHPUT_GRANULARITY))
            local climb_eval_count=0
            log "Validated median latency is still far below target, probing higher targets (max ${POST_VALIDATE_CLIMB_EVALS})"
            while (( climb_candidate <= MAX_THROUGHPUT )) && (( climb_eval_count < POST_VALIDATE_CLIMB_EVALS )); do
                evaluate_target "$step_value" "$climb_candidate" "validate_climb" "$VALIDATION_TRIALS" "$VALIDATION_RUN_DURATION"
                ((climb_eval_count += 1))
                if validation_passes; then
                    candidate="$climb_candidate"
                    final_thr="$EVAL_LAST_AVG_THR"
                    final_lat="$EVAL_LAST_AVG_LAT_US"
                    if latency_is_very_close "$final_lat"; then
                        break
                    fi
                    climb_candidate=$((climb_candidate + THROUGHPUT_GRANULARITY))
                else
                    break
                fi
            done
        fi
    else
        log "No passing throughput found under latency target ${LATENCY_TARGET_MS}ms"
    fi

    log "Search complete: slowdown_type=${SLOWDOWN_TYPE} step=${step_value} max_target=${candidate} observed_throughput=${final_thr} observed_latency_us=${final_lat} validated=${validated}"
    echo "MAX_RESULT:${candidate},${final_thr},${final_lat},${validated}"
    echo "RESULTS:${final_thr},${final_lat}"
}

trap cleanup EXIT

main() {
    local mode
    local step_value="${SLOWDOWN_VALUE}"
    local default_value
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    local idx

    mode="$(resolve_throughput_mode)"
    default_value="$(default_step_value)"
    IFS=',' read -r -a containers <<< "$(make_container_names)"

    if ! is_positive_int "$SINGLE_TRIALS"; then
        die "SINGLE_TRIALS must be a positive integer"
    fi
    if [[ "$CPU_FLAKY_MODE" == "true" ]]; then
        if [[ "$SLOWDOWN_TYPE" != "cpu" ]]; then
            die "CPU_FLAKY_MODE requires SLOWDOWN_TYPE=cpu"
        fi
        for v in "$CPU_HEALTHY_CORES" "$CPU_UNHEALTHY_CORES" "$CPU_HEALTHY_INTERVAL_SEC" "$CPU_UNHEALTHY_INTERVAL_SEC"; do
            if ! is_positive_int "$v"; then
                die "CPU flaky controls must be positive integers. Bad value: $v"
            fi
        done
    fi

    if [[ "$mode" == "search" ]]; then
        for v in "$MIN_THROUGHPUT" "$INITIAL_THROUGHPUT" "$MAX_THROUGHPUT" "$THROUGHPUT_GRANULARITY" "$MAX_EVAL_POINTS" "$FIND_TRIALS" "$VALIDATION_TRIALS" "$MAX_TRIAL_ATTEMPTS" "$FIND_RUN_DURATION" "$VALIDATION_RUN_DURATION" "$REFINE_MAX_EVALS" "$VALIDATION_BACKOFF_STEPS" "$POST_VALIDATE_CLIMB_EVALS" "$TARGET_CLOSE_TOLERANCE_US"; do
            if ! is_positive_int "$v"; then
                die "Search controls must be positive integers. Bad value: $v"
            fi
        done
        if (( MIN_THROUGHPUT > MAX_THROUGHPUT )); then
            die "MIN_THROUGHPUT must be <= MAX_THROUGHPUT"
        fi
        if (( THROUGHPUT_GRANULARITY > MAX_THROUGHPUT )); then
            die "THROUGHPUT_GRANULARITY must be <= MAX_THROUGHPUT"
        fi
    fi

    log "Running mode=${mode}, slowdown_type=${SLOWDOWN_TYPE}, step=${step_value}, client_type=${CLIENT_TYPE}"

    wait_for_cluster_ready
    warmup_cluster

    for idx in "${!systems[@]}"; do
        local target_value="$step_value"
        if ! is_slowdown_node "${systems[$idx]}"; then
            target_value="$default_value"
        fi
        apply_resource_restraints "${systems[$idx]}" "$target_value" "${containers[$idx]}"
    done
    wait_for_cluster_ready

    if [[ "$CPU_FLAKY_MODE" == "true" ]]; then
        start_cpu_flaky_loops
    fi

    if [[ "$mode" == "single" ]]; then
        run_single_target "$step_value"
    else
        run_search_mode "$step_value"
    fi
}

main
