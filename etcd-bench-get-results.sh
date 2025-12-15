#!/bin/bash

set -e

UNHBENCH_DIR="./UNHBench"
RESULTS_DIR="./results"
WORKLOAD_FILE="./workload.in"

if [ ! -d "$UNHBENCH_DIR" ]; then
    echo "Error: UNHBench directory not found at $UNHBENCH_DIR"
    exit 1
fi

mkdir -p "$RESULTS_DIR"

# Create the workload file
cat <<EOF > "$WORKLOAD_FILE"
workload=core
recordcount=1000
operationcount=10000
readproportion=0.50
updateproportion=0.50
etcd.endpoints=http://192.168.1.3:2380,http://192.168.1.4:2380,http://192.168.1.5:2380
etcd.dial_timeout=10s
etcd.request_timeout=5s
coordomission=true
maxexecutiontime=10s
threadcount=16
target=0
loadmode=duration
loadduration=10
measurement.type=raw
csvfilename=etcd_16t_10s
EOF

# Get relation between results dir and workload file
BENCH_BINARY=$(realpath "$UNHBENCH_DIR/bin/unh-bench")
REL_PATH=$(realpath --relative-to="$RESULTS_DIR" "$WORKLOAD_FILE")
RESULTS_DIR=$(realpath "$RESULTS_DIR")

# Run the benchmark
pushd "$RESULTS_DIR"
"$BENCH_BINARY" run etcd -P "$REL_PATH"

echo "Benchmark completed. Results are stored in $RESULTS_DIR"

# Use tooling to visualize results
popd

python3 "$UNHBENCH_DIR/tool/analysis/summary_stats.py" \
  -p etcd_16t_10s \
  -f "$RESULTS_DIR/" \
  -w 1

python3 "$UNHBENCH_DIR/tool/analysis/plot_servers.py" \
  -p files:etcd_16t_10s_1_sumstat.csv label:"etcd 16T" color:blue marker:. \
  -f "$(pwd)/" \
  -n etcd_16t_throughput \
  -t "etcd 16 threads (10s burst)" \
  -xs window \
  -ys OpPS \
  -x "Time window (s)" \
  -y "Throughput (ops/sec)"
