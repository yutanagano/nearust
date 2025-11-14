#!/bin/bash
echo "NOTE: make sure you are running this shell script from outside the Python venv or else rustc will complain about some linking issues"

echo "Building CLI binary..."
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

echo "Building Python package..."
source ./.venv/bin/activate
pip install .[dev]

CURRENT_COMMIT=$(python ./profiling/scripts/get_version.py)
REPEATS=5

if [ ! -d "profiling/results" ]; then
  mkdir "profiling/results"
fi

function basic_benchmarking {
  local total_time_s=0
  local total_mem_kb=0

  for i in $(seq 1 $REPEATS); do
    local output=$(/usr/bin/time -v $1 2>&1 >/dev/null)

    local time_str=$(echo "$output" | grep "Elapsed (wall clock) time" | awk -F': ' '{print $2}')
    IFS=: read -r -a parts <<< "$time_str"
    time_s=$(echo "${parts[0]} * 60 + ${parts[1]}" | bc)
    total_time_s=$(echo "$total_time_s + $time_s" | bc)

    local mem_kb=$(echo "$output" | grep "Maximum resident set size" | awk -F': ' '{print $2}')
    total_mem_kb=$(echo "$total_mem_kb + $mem_kb" | bc)
  done

  local avg_time_s=$(echo "scale=2; $total_time_s / $REPEATS" | bc)
  local avg_mem_kb=$(echo "scale=2; $total_mem_kb / $REPEATS" | bc)

  echo "avg runtime (s):            $avg_time_s"
  echo "avg max resident size (kb): $avg_mem_kb"
}

echo "Profiling within-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt")" > "./profiling/results/basic.within.${CURRENT_COMMIT}"

echo "Profiling cross-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt ./test_files/cdr3b_1m_b.txt")" > "./profiling/results/basic.cross.${CURRENT_COMMIT}"
