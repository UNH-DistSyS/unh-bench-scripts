#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

OUT_BASE="${OUT_BASE:-${SCRIPT_DIR}/out}"
TARGETS=(${TARGETS:-16000 24000 32000})
MODES=(${MODES:-cpu memory network latency})

TRIALS="${TRIALS:-1}"
MAX_TRIAL_ATTEMPTS="${MAX_TRIAL_ATTEMPTS:-5}"
CLIENT_TYPE="${CLIENT_TYPE:-hybrid}"
SILENCE="${SILENCE:-true}"
CASS_CONNECTIONS="${CASS_CONNECTIONS:-32}"

# Aggressive workload profile to push the system while preserving stable
# point collection in degraded-node experiments.
THREAD_COUNT="${THREAD_COUNT:-1024}"
RUN_DURATION="${RUN_DURATION:-60}"
RECORD_COUNT="${RECORD_COUNT:-400000}"
OPERATION_COUNT="${OPERATION_COUNT:-100000000}"

# Stable mode-specific steps (override with env vars if desired).
CPU_STEPS=(${CPU_STEPS:-8 6 4 2})
NETWORK_STEPS=(${NETWORK_STEPS:-1000 750 500 250 100})
LATENCY_STEPS=(${LATENCY_STEPS:-10us 100us 1ms 10ms})
MEMORY_STEPS=(${MEMORY_STEPS:-})

CLEAN_OUT="${CLEAN_OUT:-true}"
if [[ "${CLEAN_OUT}" == "true" ]]; then
  rm -rf "${OUT_BASE}"
fi
mkdir -p "${OUT_BASE}"
LOG_DIR="${LOG_DIR:-${OUT_BASE}/logs}"
mkdir -p "${LOG_DIR}"

failures=()

mode_steps() {
  local mode="$1"
  case "$mode" in
    cpu) printf "%s\n" "${CPU_STEPS[@]}" ;;
    network) printf "%s\n" "${NETWORK_STEPS[@]}" ;;
    latency) printf "%s\n" "${LATENCY_STEPS[@]}" ;;
    memory)
      if [[ "${#MEMORY_STEPS[@]}" -gt 0 ]]; then
        printf "%s\n" "${MEMORY_STEPS[@]}"
      fi
      ;;
    *) ;;
  esac
}

for target in "${TARGETS[@]}"; do
  out_dir="${OUT_BASE}/throughput_${target}"
  echo "===== TARGET ${target} ops/s -> ${out_dir} ====="
  for mode in "${MODES[@]}"; do
    echo "--- RUN ${mode} @ ${target} ---"
    run_ts="$(date +%Y%m%d_%H%M%S)"
    log_file="${LOG_DIR}/target_${target}_${mode}_${run_ts}.log"

    mapfile -t step_args < <(mode_steps "${mode}")

    set +e
    OUT_DIR="${out_dir}" \
      CLIENT_TYPE="${CLIENT_TYPE}" \
      SILENCE="${SILENCE}" \
      CASS_CONNECTIONS="${CASS_CONNECTIONS}" \
      WORKLOAD_TARGET="${target}" \
      THREAD_COUNT="${THREAD_COUNT}" \
      RUN_DURATION="${RUN_DURATION}" \
      RECORD_COUNT="${RECORD_COUNT}" \
      OPERATION_COUNT="${OPERATION_COUNT}" \
      TRIALS="${TRIALS}" \
      MAX_TRIAL_ATTEMPTS="${MAX_TRIAL_ATTEMPTS}" \
      SLOWDOWN_TYPE="${mode}" \
      bash "${SCRIPT_DIR}/cass.sh" "${step_args[@]}" 2>&1 | tee "${log_file}"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ "${rc}" -ne 0 ]]; then
      failures+=("target=${target},mode=${mode},log=${log_file}")
      echo "--- FAILED ${mode} @ ${target} (rc=${rc}) ---"
      echo
      continue
    fi

    echo "--- DONE ${mode} @ ${target} ---"
    echo
  done
  echo "===== DONE TARGET ${target} ====="
  echo
done

if [[ "${#failures[@]}" -gt 0 ]]; then
  echo "===== RUN COMPLETE WITH FAILURES ====="
  printf '%s\n' "${failures[@]}"
  exit 1
fi

echo "===== RUN COMPLETE: ALL TARGETS/MODES SUCCEEDED ====="
