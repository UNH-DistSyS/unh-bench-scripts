#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SSH_USER="${SSH_USER:-egp1042}"
DOCKER_CMD="${DOCKER_CMD:-docker}"

CASSANDRA_IMAGE="${CASSANDRA_IMAGE:-cassandra:5.0}"
CLUSTER_NAME="${CLUSTER_NAME:-TestCluster}"
CONTAINER_PREFIX="${CONTAINER_PREFIX:-cassandra-node}"
DATA_VOLUME_PREFIX="${DATA_VOLUME_PREFIX:-cassandra-data}"

# Hard-code the three nodes for now
TARGET_HOSTS=("ccl1.cyber.lab" "ccl2.cyber.lab" "ccl3.cyber.lab")

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

ts() { date +"[%Y-%m-%d %H:%M:%S]"; }
log() { echo "$(ts) $*"; }

ssh_run() {
  local host=$1; shift
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "${SSH_USER}@${host}" "$@"
}

get_host_ip() {
  # Resolve IPv4 address of a hostname as seen from this host.
  local host=$1
  getent ahostsv4 "${host}" | awk '/STREAM/ {print $1; exit}'
}

# ---------------------------------------------------------------------------
# Cassandra helpers
# ---------------------------------------------------------------------------

docker_rm_cassandra() {
  local host=$1
  ssh_run "${host}" "
    set -e
    if ${DOCKER_CMD} ps -a --format '{{.Names}}' | grep -q '^${CONTAINER_PREFIX}-'; then
      ${DOCKER_CMD} rm -f \$(${DOCKER_CMD} ps -a --format '{{.Names}}' | grep '^${CONTAINER_PREFIX}-') >/dev/null 2>&1 || true
    fi
    # Remove anonymous volumes that were attached to our containers
    dangling=\$(${DOCKER_CMD} volume ls -qf dangling=true || true)
    if [ -n \"\$dangling\" ]; then
      ${DOCKER_CMD} volume rm \$dangling >/dev/null 2>&1 || true
    fi
  "
}

wait_for_cql() {
  local host=$1
  local container=$2

  log "Waiting for CQL on ${host} (${container})"
  local i
  for i in $(seq 1 40); do
    if ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"SELECT now() FROM system.local\" >/dev/null 2>&1"; then
      log "CQL ready on ${host} (${container})"
      return 0
    fi
    sleep 5
  done
  echo "CQL did not become ready on ${host} (${container})"
  return 1
}

wait_for_node_normal() {
  local host=$1
  local container=$2
  local ip=$3

  log "Waiting for node ${ip} on ${host} (${container}) to reach UN (Up/Normal)"
  local i
  for i in $(seq 1 40); do
    if ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} nodetool status" 2>/dev/null | \
       awk -v ip="${ip}" '$1 ~ /^UN$/ && $2 == ip {found=1} END {exit !found}'; then
      log "Node ${ip} on ${host} is UN"
      return 0
    fi
    echo "  ... not UN yet (${i}/40), sleeping 5s"
    sleep 5
  done
  echo "Node ${ip} did not reach UN on ${host} (${container})"
  return 1
}

start_node() {
  local idx=$1
  local host=$2
  local seed_ip=$3

  local container="${CONTAINER_PREFIX}-${idx}"
  local data_volume="${DATA_VOLUME_PREFIX}-${idx}"
  local host_ip
  host_ip=$(get_host_ip "${host}")

  if [ -z "${host_ip}" ]; then
    echo "Failed to resolve IP for ${host}" >&2
    return 1
  fi

  log "Starting Cassandra on ${host} (${host_ip}) as ${container} with seeds ${seed_ip}"

  ssh_run "${host}" "
    set -e
    ${DOCKER_CMD} volume create ${data_volume} >/dev/null
    ${DOCKER_CMD} run -d --name ${container} \
      --network host \
      -e CASSANDRA_CLUSTER_NAME='${CLUSTER_NAME}' \
      -e CASSANDRA_SEEDS='${seed_ip}' \
      -e CASSANDRA_LISTEN_ADDRESS='${host_ip}' \
      -e CASSANDRA_BROADCAST_ADDRESS='${host_ip}' \
      -e CASSANDRA_RPC_ADDRESS='0.0.0.0' \
      -e CASSANDRA_NUM_TOKENS='16' \
      -v ${data_volume}:/var/lib/cassandra \
      ${CASSANDRA_IMAGE}
  " >/dev/null
}

create_schema() {
  local host=$1
  local container="${CONTAINER_PREFIX}-1"

  log "Creating keyspaces 'ycsb' and 'test' with table 'usertable' on ${host} (${container})"

  local cql="
CREATE KEYSPACE IF NOT EXISTS ycsb
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

  ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh" <<EOF
${cql}
EOF
}

wait_for_schema() {
  log "Waiting for schema agreement on all nodes"
  local i
  for i in $(seq 1 40); do
    local ok=1
    local idx
    for idx in "${!TARGET_HOSTS[@]}"; do
      local host="${TARGET_HOSTS[$idx]}"
      local container="${CONTAINER_PREFIX}-$((idx + 1))"
      if ! ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"DESCRIBE KEYSPACE ycsb\" >/dev/null 2>&1"; then
        ok=0
        break
      fi
      if ! ssh_run "${host}" "${DOCKER_CMD} exec -i ${container} cqlsh -e \"DESCRIBE KEYSPACE test\" >/dev/null 2>&1"; then
        ok=0
        break
      fi
    done
    if [ "${ok}" -eq 1 ]; then
      log "Schema agreement reached on all nodes"
      return 0
    fi
    echo "  ... schema not ready yet (${i}/40), sleeping 5s"
    sleep 5
  done
  echo "Schema did not propagate to all nodes"
  return 1
}

cluster_down() {
  log "Tearing down Cassandra containers/volumes on: ${TARGET_HOSTS[*]}"
  for host in "${TARGET_HOSTS[@]}"; do
    docker_rm_cassandra "${host}" || true
  done
  log "Teardown complete."
}

cluster_up() {
  log "Action: up"
  log "SSH user: ${SSH_USER}"
  log "Docker command: ${DOCKER_CMD}"
  log "Target hosts: ${TARGET_HOSTS[*]}"

  # Always start from a clean slate
  cluster_down

  local seed_host="${TARGET_HOSTS[0]}"
  local seed_ip
  seed_ip=$(get_host_ip "${seed_host}")
  if [ -z "${seed_ip}" ]; then
    echo "Failed to resolve IP for seed host ${seed_host}" >&2
    exit 1
  fi
  log "Seed host: ${seed_host} (${seed_ip})"

  # 1) Start seed node
  start_node 1 "${seed_host}" "${seed_ip}"
  wait_for_cql "${seed_host}" "${CONTAINER_PREFIX}-1"
  wait_for_node_normal "${seed_host}" "${CONTAINER_PREFIX}-1" "${seed_ip}"

  # 2) Start second node – wait until fully joined
  local host2="${TARGET_HOSTS[1]}"
  local host2_ip
  host2_ip=$(get_host_ip "${host2}")
  start_node 2 "${host2}" "${seed_ip}"
  wait_for_cql "${host2}" "${CONTAINER_PREFIX}-2"
  wait_for_node_normal "${host2}" "${CONTAINER_PREFIX}-2" "${host2_ip}"

  # 3) Start third node – only after node2 is fully in the ring
  local host3="${TARGET_HOSTS[2]}"
  local host3_ip
  host3_ip=$(get_host_ip "${host3}")
  start_node 3 "${host3}" "${seed_ip}"
  wait_for_cql "${host3}" "${CONTAINER_PREFIX}-3"
  wait_for_node_normal "${host3}" "${CONTAINER_PREFIX}-3" "${host3_ip}"

  log "Cluster bring-up complete. Seed node: ${seed_host} (${seed_ip})"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ACTION="${1:-up}"

case "${ACTION}" in
  up)
    cluster_up
    create_schema "${TARGET_HOSTS[0]}"
    wait_for_schema
    ;;
  down)
    cluster_down
    ;;
  *)
    echo "Usage: $0 {up|down}"
    exit 1
    ;;
esac
