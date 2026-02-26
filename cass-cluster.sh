#!/usr/bin/env bash
set -euo pipefail

SSH_USER="${SSH_USER:-$USER}"
SSH_OPTS="${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"}"
DOCKER_CMD="${DOCKER_CMD:-docker}"

CASSANDRA_IMAGE="${CASSANDRA_IMAGE:-cassandra:5.0}"
CLUSTER_NAME="${CLUSTER_NAME:-TestCluster}"
CONTAINER_PREFIX="${CONTAINER_PREFIX:-cassandra-node}"
DATA_VOLUME_PREFIX="${DATA_VOLUME_PREFIX:-cassandra-data}"
SNAPSHOT_ROOT="${SNAPSHOT_ROOT:-/tmp/cassandra-bench-snapshots}"
SNAPSHOT_NAME_DEFAULT="${SNAPSHOT_NAME_DEFAULT:-baseline}"
REMOVE_SNAPSHOTS_ON_DOWN="${REMOVE_SNAPSHOTS_ON_DOWN:-true}"
SNAPSHOT_PARALLEL="${SNAPSHOT_PARALLEL:-true}"
# Cassandra bootstrap is usually more stable when non-seed nodes join one-at-a-time.
BOOTSTRAP_PARALLEL="${BOOTSTRAP_PARALLEL:-false}"

TARGET_HOSTS_CSV="${TARGET_HOSTS_CSV:-ccl1.cyber.lab,ccl2.cyber.lab,ccl3.cyber.lab}"
IFS=',' read -r -a TARGET_HOSTS <<< "${TARGET_HOSTS_CSV}"

if [[ "${#TARGET_HOSTS[@]}" -lt 3 ]]; then
    echo "Expected at least 3 TARGET_HOSTS, got: ${TARGET_HOSTS_CSV}" >&2
    exit 1
fi

ts() { date +"[%Y-%m-%d %H:%M:%S]"; }
log() { echo "$(ts) $*"; }

container_name() {
    local idx="$1"
    echo "${CONTAINER_PREFIX}-${idx}"
}

volume_name() {
    local idx="$1"
    echo "${DATA_VOLUME_PREFIX}-${idx}"
}

snapshot_dir() {
    local snap_name="$1"
    echo "${SNAPSHOT_ROOT}/${snap_name}"
}

snapshot_file() {
    local idx="$1"
    local snap_name="$2"
    echo "$(snapshot_dir "$snap_name")/$(volume_name "$idx").tar.gz"
}

ssh_run() {
    local host="$1"
    shift
    ssh ${SSH_OPTS} "${SSH_USER}@${host}" "$@"
}

wait_for_jobs() {
    local failed=0
    local pid
    for pid in "$@"; do
        if ! wait "$pid"; then
            failed=1
        fi
    done
    return "$failed"
}

get_host_ip() {
    local host="$1"
    getent ahostsv4 "${host}" | awk '/STREAM/ {print $1; exit}'
}

docker_rm_host() {
    local host="$1"
    ssh_run "${host}" "
        set -e
        containers=\$(${DOCKER_CMD} ps -a --format '{{.Names}}' | grep '^${CONTAINER_PREFIX}-' || true)
        if [ -n \"\$containers\" ]; then
            ${DOCKER_CMD} rm -f \$containers >/dev/null 2>&1 || true
        fi
        volumes=\$(${DOCKER_CMD} volume ls --format '{{.Name}}' | grep '^${DATA_VOLUME_PREFIX}-' || true)
        if [ -n \"\$volumes\" ]; then
            ${DOCKER_CMD} volume rm -f \$volumes >/dev/null 2>&1 || true
        fi
    "
}

wait_for_cql() {
    local host="$1"
    local container="$2"
    local attempt

    for attempt in $(seq 1 120); do
        if ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"SELECT now() FROM system.local\" >/dev/null 2>&1"; then
            log "CQL ready on ${host} (${container})"
            return 0
        fi
        sleep 2
    done

    echo "CQL did not become ready on ${host} (${container})" >&2
    return 1
}

wait_for_node_normal() {
    local host="$1"
    local container="$2"
    local ip="$3"
    local attempt

    for attempt in $(seq 1 120); do
        if ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} nodetool status" 2>/dev/null | awk -v ip="${ip}" '$1 ~ /^UN$/ && $2 == ip {found=1} END {exit !found}'; then
            log "Node ${ip} on ${host} is UN"
            return 0
        fi
        sleep 2
    done

    echo "Node ${ip} did not reach UN on ${host} (${container})" >&2
    return 1
}

wait_for_node_ready() {
    local host="$1"
    local container="$2"
    local ip="$3"

    wait_for_cql "${host}" "${container}"
    wait_for_node_normal "${host}" "${container}" "${ip}"
}

wait_for_all_nodes_ready() {
    local idx
    local -a pids=()

    for idx in "${!TARGET_HOSTS[@]}"; do
        local one_based_idx="$((idx + 1))"
        local host="${TARGET_HOSTS[$idx]}"
        local container
        local ip

        container="$(container_name "${one_based_idx}")"
        ip="$(get_host_ip "${host}")"
        if [[ -z "${ip}" ]]; then
            echo "Failed to resolve IP for ${host}" >&2
            return 1
        fi

        (
            wait_for_node_ready "${host}" "${container}" "${ip}"
        ) &
        pids+=("$!")
    done

    if [[ "${#pids[@]}" -gt 0 ]] && ! wait_for_jobs "${pids[@]}"; then
        return 1
    fi
}

start_non_seed_nodes_and_wait_ready() {
    local seed_ip="$1"
    local idx

    if [[ "${BOOTSTRAP_PARALLEL}" != "true" ]]; then
        log "Starting non-seed nodes serially for stable bootstrap"
        for idx in "${!TARGET_HOSTS[@]}"; do
            local one_based_idx="$((idx + 1))"
            local host="${TARGET_HOSTS[$idx]}"
            local container
            local ip

            if [[ "${one_based_idx}" -eq 1 ]]; then
                continue
            fi

            start_node "${one_based_idx}" "${host}" "${seed_ip}"
            container="$(container_name "${one_based_idx}")"
            ip="$(get_host_ip "${host}")"
            if [[ -z "${ip}" ]]; then
                echo "Failed to resolve IP for ${host}" >&2
                return 1
            fi
            wait_for_node_ready "${host}" "${container}" "${ip}"
        done
        return 0
    fi

    log "BOOTSTRAP_PARALLEL=true: starting non-seed nodes in parallel"
    local -a start_pids=()
    local -a ready_pids=()

    for idx in "${!TARGET_HOSTS[@]}"; do
        local one_based_idx="$((idx + 1))"
        local host="${TARGET_HOSTS[$idx]}"

        if [[ "${one_based_idx}" -eq 1 ]]; then
            continue
        fi

        (
            start_node "${one_based_idx}" "${host}" "${seed_ip}"
        ) &
        start_pids+=("$!")
    done

    if [[ "${#start_pids[@]}" -gt 0 ]] && ! wait_for_jobs "${start_pids[@]}"; then
        echo "One or more node start jobs failed" >&2
        return 1
    fi

    for idx in "${!TARGET_HOSTS[@]}"; do
        local one_based_idx="$((idx + 1))"
        local host="${TARGET_HOSTS[$idx]}"
        local container
        local ip

        if [[ "${one_based_idx}" -eq 1 ]]; then
            continue
        fi

        container="$(container_name "${one_based_idx}")"
        ip="$(get_host_ip "${host}")"
        if [[ -z "${ip}" ]]; then
            echo "Failed to resolve IP for ${host}" >&2
            return 1
        fi

        (
            wait_for_node_ready "${host}" "${container}" "${ip}"
        ) &
        ready_pids+=("$!")
    done

    if [[ "${#ready_pids[@]}" -gt 0 ]] && ! wait_for_jobs "${ready_pids[@]}"; then
        echo "One or more node readiness checks failed" >&2
        return 1
    fi
}

start_node() {
    local idx="$1"
    local host="$2"
    local seed_ip="$3"
    local container
    local volume
    local host_ip

    container="$(container_name "$idx")"
    volume="$(volume_name "$idx")"
    host_ip="$(get_host_ip "${host}")"

    if [[ -z "${host_ip}" ]]; then
        echo "Failed to resolve IP for ${host}" >&2
        return 1
    fi

    log "Starting ${container} on ${host} (${host_ip}) with seed ${seed_ip}"
    ssh_run "${host}" "
        set -e
        ${DOCKER_CMD} volume create ${volume} >/dev/null
        ${DOCKER_CMD} run -d --name ${container} \\
            --network host \\
            -e CASSANDRA_CLUSTER_NAME='${CLUSTER_NAME}' \\
            -e CASSANDRA_SEEDS='${seed_ip}' \\
            -e CASSANDRA_LISTEN_ADDRESS='${host_ip}' \\
            -e CASSANDRA_BROADCAST_ADDRESS='${host_ip}' \\
            -e CASSANDRA_RPC_ADDRESS='0.0.0.0' \\
            -e CASSANDRA_NUM_TOKENS='16' \\
            -v ${volume}:/var/lib/cassandra \\
            ${CASSANDRA_IMAGE} >/dev/null
    "
}

create_schema() {
    local host="$1"
    local container
    local cql

    container="$(container_name 1)"

    cql="
CREATE KEYSPACE IF NOT EXISTS ycsb
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
ALTER KEYSPACE ycsb
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS ycsb.usertable (
    ycsb_key text PRIMARY KEY,
    field0 text,
    field1 text,
    field2 text,
    field3 text,
    field4 text,
    field5 text,
    field6 text,
    field7 text,
    field8 text,
    field9 text
);

CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
ALTER KEYSPACE test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS test.usertable (
    ycsb_key text PRIMARY KEY,
    field0 text,
    field1 text,
    field2 text,
    field3 text,
    field4 text,
    field5 text,
    field6 text,
    field7 text,
    field8 text,
    field9 text
);
"

    log "Creating schema on ${host} (${container})"
    ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh" <<EOCQL
${cql}
EOCQL
}

wait_for_schema() {
    local attempt
    local idx

    for attempt in $(seq 1 90); do
        local -a pids=()
        for idx in "${!TARGET_HOSTS[@]}"; do
            local host="${TARGET_HOSTS[$idx]}"
            local container
            container="$(container_name "$((idx + 1))")"

            (
                ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"DESCRIBE KEYSPACE ycsb\" >/dev/null 2>&1"
                ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"DESCRIBE KEYSPACE test\" >/dev/null 2>&1"
            ) &
            pids+=("$!")
        done

        if wait_for_jobs "${pids[@]}"; then
            log "Schema available on all nodes"
            return 0
        fi
        sleep 2
    done

    echo "Schema did not propagate to all nodes" >&2
    return 1
}

cluster_down_internal() {
    local preserve_snapshots="$1"
    local host
    local -a pids=()

    log "Tearing down Cassandra containers/volumes on: ${TARGET_HOSTS[*]}"
    for host in "${TARGET_HOSTS[@]}"; do
        (
            docker_rm_host "${host}" || true
        ) &
        pids+=("$!")
    done
    if [[ "${#pids[@]}" -gt 0 ]]; then
        wait_for_jobs "${pids[@]}" || true
    fi

    if [[ "${preserve_snapshots}" != "true" ]]; then
        pids=()
        for host in "${TARGET_HOSTS[@]}"; do
            (
                log "Removing snapshots on ${host}: ${SNAPSHOT_ROOT}"
                ssh_run "${host}" "rm -rf '${SNAPSHOT_ROOT}'" || true
            ) &
            pids+=("$!")
        done
        if [[ "${#pids[@]}" -gt 0 ]]; then
            wait_for_jobs "${pids[@]}" || true
        fi
    fi

    log "Cluster teardown complete"
}

cluster_up() {
    local seed_host
    local seed_ip

    cluster_down_internal true

    seed_host="${TARGET_HOSTS[0]}"
    seed_ip="$(get_host_ip "${seed_host}")"
    if [[ -z "${seed_ip}" ]]; then
        echo "Failed to resolve seed IP for ${seed_host}" >&2
        exit 1
    fi

    start_node 1 "${seed_host}" "${seed_ip}"
    wait_for_node_ready "${seed_host}" "$(container_name 1)" "${seed_ip}"
    start_non_seed_nodes_and_wait_ready "${seed_ip}"

    create_schema "${seed_host}"
    wait_for_schema
    log "Cluster bring-up complete"
}

snapshot_create() {
    local snap_name="${1:-${SNAPSHOT_NAME_DEFAULT}}"
    local idx
    local -a pids=()

    log "Creating snapshot '${snap_name}'"
    for idx in "${!TARGET_HOSTS[@]}"; do
        local one_based_idx="$((idx + 1))"
        local host="${TARGET_HOSTS[$idx]}"
        local container
        local volume
        local snap_dir
        local snap_file

        container="$(container_name "${one_based_idx}")"
        volume="$(volume_name "${one_based_idx}")"
        snap_dir="$(snapshot_dir "${snap_name}")"
        snap_file="$(snapshot_file "${one_based_idx}" "${snap_name}")"

        log "Snapshotting ${host}:${container} volume ${volume} -> ${snap_file}"
        if [[ "${SNAPSHOT_PARALLEL}" == "true" ]]; then
            (
                ssh_run "${host}" "
                    set -euo pipefail
                    mkdir -p '${snap_dir}'
                    ${DOCKER_CMD} stop '${container}' >/dev/null
                    rm -f '${snap_file}'
                    ${DOCKER_CMD} run --rm \\
                        -v '${volume}:/from' \\
                        -v '${snap_dir}:/to' \\
                        '${CASSANDRA_IMAGE}' \\
                        bash -lc 'cd /from && tar -czf /to/$(basename "${snap_file}") .'
                    ${DOCKER_CMD} start '${container}' >/dev/null
                "
            ) &
            pids+=("$!")
        else
            ssh_run "${host}" "
                set -euo pipefail
                mkdir -p '${snap_dir}'
                ${DOCKER_CMD} stop '${container}' >/dev/null
                rm -f '${snap_file}'
                ${DOCKER_CMD} run --rm \\
                    -v '${volume}:/from' \\
                    -v '${snap_dir}:/to' \\
                    '${CASSANDRA_IMAGE}' \\
                    bash -lc 'cd /from && tar -czf /to/$(basename "${snap_file}") .'
                ${DOCKER_CMD} start '${container}' >/dev/null
            "
        fi
    done

    if [[ "${SNAPSHOT_PARALLEL}" == "true" ]] && [[ "${#pids[@]}" -gt 0 ]]; then
        if ! wait_for_jobs "${pids[@]}"; then
            echo "One or more snapshot-create jobs failed" >&2
            exit 1
        fi
    fi

    if ! wait_for_all_nodes_ready; then
        echo "One or more snapshot-create readiness checks failed" >&2
        exit 1
    fi

    wait_for_schema
    log "Snapshot '${snap_name}' created"
}

snapshot_restore() {
    local snap_name="${1:-${SNAPSHOT_NAME_DEFAULT}}"
    local seed_host
    local seed_ip
    local idx
    local -a pids=()

    log "Restoring snapshot '${snap_name}'"

    cluster_down_internal true

    for idx in "${!TARGET_HOSTS[@]}"; do
        local one_based_idx="$((idx + 1))"
        local host="${TARGET_HOSTS[$idx]}"
        local volume
        local snap_dir
        local snap_file

        volume="$(volume_name "${one_based_idx}")"
        snap_dir="$(snapshot_dir "${snap_name}")"
        snap_file="$(snapshot_file "${one_based_idx}" "${snap_name}")"

        log "Restoring ${host}:${volume} from ${snap_file}"
        if [[ "${SNAPSHOT_PARALLEL}" == "true" ]]; then
            (
                ssh_run "${host}" "
                    set -euo pipefail
                    if [ ! -f '${snap_file}' ]; then
                        echo 'Snapshot file not found: ${snap_file}' >&2
                        exit 1
                    fi
                    ${DOCKER_CMD} volume create '${volume}' >/dev/null
                    ${DOCKER_CMD} run --rm \\
                        -v '${volume}:/to' \\
                        -v '${snap_dir}:/from' \\
                        '${CASSANDRA_IMAGE}' \\
                        bash -lc 'cd /to && tar -xzf /from/$(basename "${snap_file}")'
                "
            ) &
            pids+=("$!")
        else
            ssh_run "${host}" "
                set -euo pipefail
                if [ ! -f '${snap_file}' ]; then
                    echo 'Snapshot file not found: ${snap_file}' >&2
                    exit 1
                fi
                ${DOCKER_CMD} volume create '${volume}' >/dev/null
                ${DOCKER_CMD} run --rm \\
                    -v '${volume}:/to' \\
                    -v '${snap_dir}:/from' \\
                    '${CASSANDRA_IMAGE}' \\
                    bash -lc 'cd /to && tar -xzf /from/$(basename "${snap_file}")'
            "
        fi
    done

    if [[ "${SNAPSHOT_PARALLEL}" == "true" ]] && [[ "${#pids[@]}" -gt 0 ]]; then
        if ! wait_for_jobs "${pids[@]}"; then
            echo "One or more snapshot-restore jobs failed" >&2
            exit 1
        fi
    fi

    seed_host="${TARGET_HOSTS[0]}"
    seed_ip="$(get_host_ip "${seed_host}")"
    if [[ -z "${seed_ip}" ]]; then
        echo "Failed to resolve seed IP for ${seed_host}" >&2
        exit 1
    fi

    start_node 1 "${seed_host}" "${seed_ip}"
    wait_for_node_ready "${seed_host}" "$(container_name 1)" "${seed_ip}"
    start_non_seed_nodes_and_wait_ready "${seed_ip}"

    wait_for_schema
    log "Snapshot '${snap_name}' restore complete"
}

snapshot_delete() {
    local snap_name="${1:-${SNAPSHOT_NAME_DEFAULT}}"
    local host
    local -a pids=()
    for host in "${TARGET_HOSTS[@]}"; do
        (
            log "Deleting snapshot '${snap_name}' on ${host}"
            ssh_run "${host}" "rm -rf '$(snapshot_dir "${snap_name}")'"
        ) &
        pids+=("$!")
    done
    if [[ "${#pids[@]}" -gt 0 ]] && ! wait_for_jobs "${pids[@]}"; then
        echo "One or more snapshot-delete jobs failed" >&2
        exit 1
    fi
}

snapshot_list() {
    local host
    for host in "${TARGET_HOSTS[@]}"; do
        echo "${host}:"
        ssh_run "${host}" "ls -1 '${SNAPSHOT_ROOT}' 2>/dev/null || true"
    done
}

ACTION="${1:-up}"
ARG1="${2:-}"

case "${ACTION}" in
    up)
        cluster_up
        ;;
    down)
        if [[ "${REMOVE_SNAPSHOTS_ON_DOWN}" == "true" ]]; then
            cluster_down_internal false
        else
            cluster_down_internal true
        fi
        ;;
    snapshot-create)
        snapshot_create "${ARG1}"
        ;;
    snapshot-restore)
        snapshot_restore "${ARG1}"
        ;;
    snapshot-delete)
        snapshot_delete "${ARG1}"
        ;;
    snapshot-list)
        snapshot_list
        ;;
    *)
        echo "Usage: $0 {up|down|snapshot-create [name]|snapshot-restore [name]|snapshot-delete [name]|snapshot-list}" >&2
        exit 1
        ;;
esac
