#!/bin/bash

set -eou pipefail

SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"}
BENCH_HOST="${BENCH_HOST:-ccl4.cyber.lab}"
BENCH_USER="${BENCH_USER:-$USER}"
BENCH_DIR_ABSOLUTE="${BENCH_DIR_ABSOLUTE:-/home/$BENCH_USER/UNHBench}"
RESULTS_DIR="${RESULTS_DIR:-/home/$BENCH_USER/cass-results}"
WORKLOAD_FILE="${WORKLOAD_FILE:-/tmp/cass-workload.in}"
BENCH_SYSTEMS=("ccl1.cyber.lab" "ccl2.cyber.lab" "ccl3.cyber.lab")
SLOWDOWN_TYPE="${SLOWDOWN_TYPE:-cpu}"
SLOWDOWN_VALUE="${SLOWDOWN_VALUE:-${CONTAINER_CORES:-4}}"
CONTAINER_CORES="${CONTAINER_CORES:-$SLOWDOWN_VALUE}"
DEFAULT_CORES="${DEFAULT_CORES:-8}"
DEFAULT_BANDWIDTH_MBIT="${DEFAULT_BANDWIDTH_MBIT:-1000}"
DEFAULT_LATENCY_MS="${DEFAULT_LATENCY_MS:-0ms}"
DEFAULT_MEMORY_MB="${DEFAULT_MEMORY_MB:-49152}"
CASS_CONNECTIONS="${CASS_CONNECTIONS:-8}"
SLOWDOWN_NODES_CSV="${SLOWDOWN_NODES_CSV:-}"
CASS_KEYSPACE="${CASS_KEYSPACE:-ycsb}"
CASS_TABLE="${CASS_TABLE:-usertable}"
CASS_LOCAL_DC="${CASS_LOCAL_DC:-datacenter1}"
CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"
SILENCE="${SILENCE:-true}"
WORKLOAD_TARGET="${WORKLOAD_TARGET:-512000}"
THREAD_COUNT="${THREAD_COUNT:-512}"
RUN_DURATION="${RUN_DURATION:-30}"
RECORD_COUNT="${RECORD_COUNT:-200000}"
OPERATION_COUNT="${OPERATION_COUNT:-2000000}"

SLOWDOWN_NODES=()
if [[ -n "$SLOWDOWN_NODES_CSV" ]]; then
    IFS=',' read -r -a SLOWDOWN_NODES <<< "$SLOWDOWN_NODES_CSV"
fi

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

make_systems_list() {
    # local IFS=,
    # echo "${BENCH_SYSTEMS[*]}"

    # Echo the systems as a comma-separated list with host:9042 port
    local systems=()
    local host
    for host in "${BENCH_SYSTEMS[@]}"; do
        systems+=("${host}:9042")
    done
    local IFS=,
    echo "${systems[*]}"
}

make_container_names() {
    local prefix="cassandra-node"
    local names=()

    for idx in "${!BENCH_SYSTEMS[@]}"; do
        names+=("${prefix}-$(($idx + 1))")
    done

    local IFS=,
    echo "${names[*]}"
}

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

# apply_core_restraints <host> <# cores> <container_name>
apply_core_restraints() {
    local host="$1"
    local numCores="$2"
    local docker_cores="0-$(($numCores - 1))"
    local container_name="$3"

    log "Applying CPU core restraints on $host to use cores: $docker_cores"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker update --cpuset-cpus=\"$docker_cores\" \"$container_name\"" >/dev/null 2>&1
}

# remove_core_restraints <host> <container_name>
remove_core_restraints() {
    local host="$1"
    local container_name="$2"

    log "Removing CPU core restraints on $host"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker update --cpuset-cpus=\"\" \"$container_name\"" >/dev/null 2>&1
}

apply_bandwidth_restraints() {
    local host="$1"
    local target_mbit="$2"
    local container_name="$3"
    local iface=""

    iface="$(default_interface_for_host "$host")"
    if [[ -z "$iface" ]]; then
        log "ERROR: Could not determine default network interface on $host for $container_name"
        exit 1
    fi

    log "Applying bandwidth restraint on $host ($container_name): ${target_mbit}mbit on interface $iface"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" \
        "sudo tc qdisc replace dev \"$iface\" root handle 1: htb default 10 && \
         sudo tc class replace dev \"$iface\" parent 1: classid 1:10 htb rate ${target_mbit}mbit ceil ${target_mbit}mbit" >/dev/null 2>&1
}

remove_bandwidth_restraints() {
    local host="$1"
    local container_name="$2"
    local iface=""

    iface="$(default_interface_for_host "$host")"
    if [[ -z "$iface" ]]; then
        log "WARNING: Could not determine default network interface on $host while removing bandwidth restraint for $container_name"
        return
    fi

    log "Removing bandwidth restraint on $host ($container_name) from interface $iface"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc del dev \"$iface\" root >/dev/null 2>&1 || true" >/dev/null 2>&1
}

default_interface_for_host() {
    local host="$1"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "ip -o -4 route show to default | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p' | head -n1" 2>/dev/null
}

latency_is_zero() {
    local val="$1"
    [[ "$val" =~ ^0+([a-zA-Z]+)?$ ]]
}

apply_latency_restraints() {
    local host="$1"
    local target_latency_ms="$2"
    local container_name="$3"
    local iface=""

    iface="$(default_interface_for_host "$host")"
    if [[ -z "$iface" ]]; then
        log "ERROR: Could not determine default network interface on $host for $container_name"
        exit 1
    fi

    if latency_is_zero "$target_latency_ms"; then
        log "Clearing latency restraint on $host ($container_name) for interface $iface"
        ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc del dev \"$iface\" root >/dev/null 2>&1 || true" >/dev/null 2>&1
        return
    fi

    log "Applying latency restraint on $host ($container_name): ${target_latency_ms} on interface $iface"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" \
        "sudo tc qdisc replace dev \"$iface\" root netem delay ${target_latency_ms}" >/dev/null 2>&1
}

remove_latency_restraints() {
    local host="$1"
    local container_name="$2"
    local iface=""

    iface="$(default_interface_for_host "$host")"
    if [[ -z "$iface" ]]; then
        log "WARNING: Could not determine default network interface on $host while removing latency restraint for $container_name"
        return
    fi

    log "Removing latency restraint on $host ($container_name) from interface $iface"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "sudo tc qdisc del dev \"$iface\" root >/dev/null 2>&1 || true" >/dev/null 2>&1
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
        log "Clearing memory pressure on $host (target availability ${target_available_mb}MB)"
        ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f \"$memhog_file\"" >/dev/null 2>&1
        return
    fi

    log "Applying memory pressure on $host: reserving ${hog_mb}MB in tmpfs (target availability ${target_available_mb}MB)"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f \"$memhog_file\" && dd if=/dev/zero of=\"$memhog_file\" bs=1M count=$hog_mb status=none" >/dev/null 2>&1
}

remove_memory_restraints() {
    local host="$1"
    local container_name="$2"
    local host_tag="${host%%.*}"
    local memhog_file="/dev/shm/cass_memhog_${host_tag}.bin"

    log "Clearing memory pressure on $host for $container_name"
    ssh ${SSH_OPTS} "$BENCH_USER@$host" "rm -f \"$memhog_file\"" >/dev/null 2>&1
}

wait_for_cluster_ready() {
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    IFS=',' read -r -a containers <<< "$(make_container_names)"

    log "Waiting for Cassandra containers to accept CQL after ${SLOWDOWN_TYPE} update"
    local idx
    for idx in "${!systems[@]}"; do
        local host="${systems[$idx]}"
        local container="${containers[$idx]}"
        local attempt
        for attempt in $(seq 1 60); do
            if ssh ${SSH_OPTS} "$BENCH_USER@$host" "docker exec -i \"$container\" cqlsh -e \"SELECT now() FROM system.local\" >/dev/null 2>&1"; then
                log "CQL ready on $host ($container)"
                break
            fi
            if [[ "$attempt" -eq 60 ]]; then
                log "ERROR: CQL did not become ready on $host ($container) after slowdown update"
                exit 1
            fi
            sleep 2
        done
    done
}

default_step_value() {
    case "$SLOWDOWN_TYPE" in
        cpu) echo "$DEFAULT_CORES" ;;
        network) echo "$DEFAULT_BANDWIDTH_MBIT" ;;
        latency) echo "$DEFAULT_LATENCY_MS" ;;
        memory) echo "$DEFAULT_MEMORY_MB" ;;
        *)
            log "ERROR: Unsupported SLOWDOWN_TYPE '$SLOWDOWN_TYPE' (expected: cpu, network, latency, or memory)"
            exit 1
            ;;
    esac
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
        *)
            log "ERROR: Unsupported SLOWDOWN_TYPE '$SLOWDOWN_TYPE' (expected: cpu, network, latency, or memory)"
            exit 1
            ;;
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
        *)
            log "ERROR: Unsupported SLOWDOWN_TYPE '$SLOWDOWN_TYPE' (expected: cpu, network, latency, or memory)"
            exit 1
            ;;
    esac
}

run_experiment() {
    local results_dir="$RESULTS_DIR/${SLOWDOWN_TYPE}_$1"
    local prefix="cassandra_${SLOWDOWN_TYPE}_$1"
    local systems_list
    systems_list="$(make_systems_list)"
    ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" \
        "CASS_BENCH_DIR_ABSOLUTE=\"$BENCH_DIR_ABSOLUTE\" CASS_SYSTEMS_LIST=\"$systems_list\" CASS_RESULTS_DIR=\"$results_dir\" CASS_PREFIX=\"$prefix\" CASS_KEYSPACE=\"$CASS_KEYSPACE\" CASS_TABLE=\"$CASS_TABLE\" CASS_LOCAL_DC=\"$CASS_LOCAL_DC\" CASS_CLIENT_TYPE=\"$CLIENT_TYPE\" CASS_SILENCE=\"$SILENCE\" CASS_WORKLOAD_TARGET=\"$WORKLOAD_TARGET\" CASS_THREAD_COUNT=\"$THREAD_COUNT\" CASS_RUN_DURATION=\"$RUN_DURATION\" CASS_RECORD_COUNT=\"$RECORD_COUNT\" CASS_OPERATION_COUNT=\"$OPERATION_COUNT\" CASS_CONNECTIONS=\"$CASS_CONNECTIONS\" bash -s" <<'EOF'

set -euo pipefail

rm -rf "$CASS_RESULTS_DIR" 2>/dev/null || true
mkdir -p "$CASS_RESULTS_DIR"

cat > "workload_${CASS_PREFIX}.in" <<EOW
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
cassandra.writeconsistencylevel=ONE
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

WORKLOAD_FILE_ABSOLUTE=$(pwd)/"workload_${CASS_PREFIX}.in"

trap 'rm -f "$WORKLOAD_FILE_ABSOLUTE"' EXIT

pushd "$CASS_RESULTS_DIR" >/dev/null
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting load phase"
LOAD_LOG="${CASS_RESULTS_DIR}/${CASS_PREFIX}_load.log"
RUN_LOG="${CASS_RESULTS_DIR}/${CASS_PREFIX}_run.log"

if ! "$CASS_BENCH_DIR_ABSOLUTE/bin/unh-bench" load cassandra -P "$WORKLOAD_FILE_ABSOLUTE" >"$LOAD_LOG" 2>&1; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Load phase failed. See $LOAD_LOG"
    tail -n 50 "$LOAD_LOG" || true
    exit 1
fi

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Load phase complete"
# Keep load only for data priming; clear load CSVs so run metrics are isolated.
rm -f "${CASS_PREFIX}_primary_"*.csv 2>/dev/null || true
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting run phase"
if ! "$CASS_BENCH_DIR_ABSOLUTE/bin/unh-bench" run cassandra -P "$WORKLOAD_FILE_ABSOLUTE" >"$RUN_LOG" 2>&1; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Run phase failed. See $RUN_LOG"
    tail -n 50 "$RUN_LOG" || true
    exit 1
fi
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Run phase complete"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] UNHBench logs: $LOAD_LOG $RUN_LOG"
popd >/dev/null

EOF

}

process_results() {
    local results_dir="$RESULTS_DIR/${SLOWDOWN_TYPE}_$1"
    local prefix="cassandra_${SLOWDOWN_TYPE}_$1"
    log "Processing results in $results_dir"
    ssh ${SSH_OPTS} "$BENCH_USER@$BENCH_HOST" \
       "CASS_STEP_VALUE=\"$1\" CASS_BENCH_DIR_ABSOLUTE=\"$BENCH_DIR_ABSOLUTE\" CASS_RESULTS_DIR=\"$results_dir\" CASS_PREFIX=\"$prefix\" bash -s" <<'EOF'

set -euo pipefail

pushd "$CASS_RESULTS_DIR" >/dev/null

shopt -s nullglob
raw_files=( "${CASS_RESULTS_DIR}/${CASS_PREFIX}_primary_"*.csv )
if (( ${#raw_files[@]} == 0 )); then
    echo "ERROR: No raw run CSV files found for prefix ${CASS_PREFIX} in ${CASS_RESULTS_DIR}" >&2
    exit 1
fi

read -r total_ops error_ops <<< "$(awk -F, 'FNR > 1 {tot += 1; if ($1 ~ /_ERROR$/) err += 1} END {printf "%d %d", tot + 0, err + 0}' "${raw_files[@]}")"
if (( total_ops == 0 )); then
    echo "ERROR: Raw run CSV files exist but contain zero operations for prefix ${CASS_PREFIX}" >&2
    exit 1
fi

if (( error_ops > 0 )); then
    error_pct="$(awk -v e="$error_ops" -v t="$total_ops" 'BEGIN {printf "%.2f", (e * 100.0) / t}')"
    echo "ERROR: Found ${error_ops}/${total_ops} operations with *_ERROR (${error_pct}%) in raw run CSVs for prefix ${CASS_PREFIX}" >&2
    echo "ERROR: Sample failing rows:" >&2
    awk -F, 'FNR > 1 && $1 ~ /_ERROR$/ {print FILENAME ":" FNR ":" $0; n += 1; if (n == 5) exit}' "${raw_files[@]}" >&2
    exit 1
fi

python3 "$CASS_BENCH_DIR_ABSOLUTE/tool/analysis/summary_stats.py" -p "$CASS_PREFIX" -f "$CASS_RESULTS_DIR/" -w 1
cp "${CASS_RESULTS_DIR}/${CASS_PREFIX}_1_sumstat.csv" "${CASS_RESULTS_DIR}/${CASS_PREFIX}.csv"

avg_throughput=$(awk -F, 'NR > 1 && $2 > 0 {sum += $2; n += 1} END {if (n > 0) printf "%.4f", sum / n; else print "N/A"}' "${CASS_RESULTS_DIR}/${CASS_PREFIX}.csv")
avg_latency=$(awk -F, 'NR > 1 && $2 > 0 && $3 > 0 {weighted += ($2 * $3); ops += $2} END {if (ops > 0) printf "%.4f", weighted / ops; else print "N/A"}' "${CASS_RESULTS_DIR}/${CASS_PREFIX}.csv")

popd >/dev/null

# Output summary
echo "RESULTS:${CASS_STEP_VALUE},${avg_throughput},${avg_latency}"
EOF

}

cleanup() {
    set +e
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    IFS=',' read -r -a containers <<< "$(make_container_names)"

    for idx in "${!systems[@]}"; do
        remove_resource_restraints "${systems[$idx]}" "${containers[$idx]}"
    done

    set -e
}

trap cleanup EXIT

main() {
    local step_value="$1"
    local default_value
    default_value="$(default_step_value)"
    local systems=("${BENCH_SYSTEMS[@]}")
    local containers
    IFS=',' read -r -a containers <<< "$(make_container_names)"

    for idx in "${!systems[@]}"; do
        local target_value="$step_value"
        if ! is_slowdown_node "${systems[$idx]}"; then
            target_value="$default_value"
        fi
        apply_resource_restraints "${systems[$idx]}" "$target_value" "${containers[$idx]}"
    done
    wait_for_cluster_ready

    run_experiment "$step_value"
    process_results "$step_value"
}

main "$SLOWDOWN_VALUE"
