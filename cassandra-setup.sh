#!/bin/bash

# Bring up a 3-node Cassandra cluster across three hosts via Docker (YCSB-ready).
# Usage: ./cassandra-setup.sh [host1 host2 host3]
# Env: SSH_USER, CASSANDRA_VERSION, CLUSTER_NAME, CONTAINER_NAME_PREFIX, DATA_VOLUME_PREFIX, DC_NAME, RACK_NAME, DOCKER_CMD.

set -euo pipefail

SYSTEMS=("ccl1.cyber.lab" "ccl2.cyber.lab" "ccl3.cyber.lab")

if (($# > 0)); then
    SYSTEMS=("$@")
fi

if [ "${#SYSTEMS[@]}" -ne 3 ]; then
    echo "Expected 3 hosts (override defaults by passing hostnames/IPs as arguments)." >&2
    exit 1
fi

CASSANDRA_VERSION="${CASSANDRA_VERSION:-latest}"
CLUSTER_NAME="${CLUSTER_NAME:-ycsb-cluster}"
SSH_USER="${SSH_USER:-$USER}"
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"}
CONTAINER_NAME_PREFIX="${CONTAINER_NAME_PREFIX:-cassandra-node}"
DATA_VOLUME_PREFIX="${DATA_VOLUME_PREFIX:-cass-data}"
DC_NAME="${DC_NAME:-dc1}"
RACK_NAME="${RACK_NAME:-rack1}"
DOCKER_CMD="${DOCKER_CMD:-docker}"
KEYSPACE="${KEYSPACE:-ycsb}"
TABLE_NAME="${TABLE_NAME:-usertable}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
SCHEMA_RETRIES="${SCHEMA_RETRIES:-30}"
SCHEMA_SLEEP="${SCHEMA_SLEEP:-5}"
SKIP_SCHEMA="${SKIP_SCHEMA:-0}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

run_remote() {
    local host="$1"
    shift
    ssh ${SSH_OPTS} "$SSH_USER@$host" "$@"
}

get_host_ip() {
    local host="$1"
    run_remote "$host" "set -- \$(hostname -I); echo \$1"
}

ensure_docker() {
    local host="$1"
    run_remote "$host" "$DOCKER_CMD info >/dev/null"
}

start_node() {
    local host="$1"
    local ip="$2"
    local seeds="$3"
    local node_name="$4"
    local volume_name="$5"

    log "Starting Cassandra on $host ($ip) as $node_name"
    ssh ${SSH_OPTS} "$SSH_USER@$host" "bash -s" <<EOF
set -euo pipefail
DOCKER_CMD="$DOCKER_CMD"
$DOCKER_CMD pull cassandra:$CASSANDRA_VERSION
$DOCKER_CMD rm -f $node_name >/dev/null 2>&1 || true
$DOCKER_CMD volume create $volume_name >/dev/null
# Host networking keeps gossip/CQL ports reachable across machines without explicit port mappings.
$DOCKER_CMD run -d --name $node_name --hostname $node_name --restart unless-stopped --network host \
  -v $volume_name:/var/lib/cassandra \
  -e CASSANDRA_CLUSTER_NAME=$CLUSTER_NAME \
  -e CASSANDRA_SEEDS=$seeds \
  -e CASSANDRA_LISTEN_ADDRESS=$ip \
  -e CASSANDRA_BROADCAST_ADDRESS=$ip \
  -e CASSANDRA_BROADCAST_RPC_ADDRESS=$ip \
  -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
  -e CASSANDRA_DC=$DC_NAME \
  -e CASSANDRA_RACK=$RACK_NAME \
  cassandra:$CASSANDRA_VERSION
EOF
}

wait_for_cql() {
    local host="$1"
    local container="$2"

    log "Waiting for CQL on $host ($container)"
    for _ in $(seq 1 "$SCHEMA_RETRIES"); do
        if ssh ${SSH_OPTS} "$SSH_USER@$host" "$DOCKER_CMD exec \"$container\" cqlsh -e \"SELECT now() FROM system.local;\" >/dev/null 2>&1"; then
            log "CQL is up on $host"
            return 0
        fi
        sleep "$SCHEMA_SLEEP"
    done
    echo "CQL did not become ready on $host after $SCHEMA_RETRIES attempts" >&2
    return 1
}

create_schema() {
    local host="$1"
    local container="$2"

    wait_for_cql "$host" "$container"

    log "Creating keyspace/table on $host"
    ssh ${SSH_OPTS} "$SSH_USER@$host" "bash -s" <<EOF
set -euo pipefail
DOCKER_CMD="$DOCKER_CMD"
KS="$KEYSPACE"
TBL="$TABLE_NAME"
RF="$REPLICATION_FACTOR"

$DOCKER_CMD exec "$container" cqlsh -e "CREATE KEYSPACE IF NOT EXISTS \\"\$KS\\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': \$RF};"
$DOCKER_CMD exec "$container" cqlsh -k "\$KS" -e "CREATE TABLE IF NOT EXISTS \\"\$TBL\\" (y_id text PRIMARY KEY, field0 text, field1 text, field2 text, field3 text, field4 text, field5 text, field6 text, field7 text, field8 text, field9 text);"
EOF
}

log "Using SSH user: $SSH_USER"
log "Docker command on remote hosts: $DOCKER_CMD"
log "Target hosts: ${SYSTEMS[*]}"

HOST_IPS=()
for host in "${SYSTEMS[@]}"; do
    log "Checking docker on $host"
    ensure_docker "$host"

    log "Resolving IP for $host"
    ip=$(get_host_ip "$host")
    if [ -z "$ip" ]; then
        echo "Could not determine IP for $host" >&2
        exit 1
    fi
    HOST_IPS+=("$ip")
done

SEED_IP="${HOST_IPS[0]}"
SEED_CONTAINER="${CONTAINER_NAME_PREFIX}-1"

index=1
for host_index in "${!SYSTEMS[@]}"; do
    host="${SYSTEMS[$host_index]}"
    ip="${HOST_IPS[$host_index]}"
    node_name="${CONTAINER_NAME_PREFIX}-${index}"
    volume_name="${DATA_VOLUME_PREFIX}-${index}"
    start_node "$host" "$ip" "$SEED_IP" "$node_name" "$volume_name"
    index=$((index + 1))
done

if [ "$SKIP_SCHEMA" != "1" ]; then
    create_schema "${SYSTEMS[0]}" "$SEED_CONTAINER"
else
    log "Skipping schema creation (SKIP_SCHEMA=$SKIP_SCHEMA)"
fi

host_csv=$(IFS=,; echo "${HOST_IPS[*]}")

log "Cluster bring-up requested. Seed node: ${SYSTEMS[0]} ($SEED_IP)"
echo "YCSB contact points (hosts property): $host_csv"
echo "Example: ./bin/ycsb load cassandra-cql -p hosts=$host_csv -P workloads/workloada"
