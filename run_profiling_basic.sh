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

mean=0
std=0
function compute_mean_std {
  local vals=("$@")

  local sum=0
  for val in "${vals[@]}"; do
    sum=$(echo "$sum + $val" | bc -l)
  done
  mean=$(echo "$sum / ${#vals[@]}" | bc -l)

  local sum=0
  for val in "${vals[@]}"; do
    sum=$(echo "$sum + ($val - $mean) * ($val - $mean)" | bc -l)
  done
  std=$(echo "sqrt($sum / ${#vals[@]})" | bc -l)
}

function basic_benchmarking {
  local time_s_array=()
  local mem_kb_array=()

  for i in $(seq 1 $REPEATS); do
    local output=$(/usr/bin/time -v $1 2>&1 >/dev/null)

    local time_str=$(echo "$output" | grep "Elapsed (wall clock) time" | awk -F': ' '{print $2}')
    IFS=: read -r -a parts <<< "$time_str"
    time_s=$(echo "${parts[0]} * 60 + ${parts[1]}" | bc -l)
    time_s_array+=($time_s)

    local mem_kb=$(echo "$output" | grep "Maximum resident set size" | awk -F': ' '{print $2}')
    mem_kb_array+=($mem_kb)
  done

  compute_mean_std "${time_s_array[@]}"
  local mean_time_s=$mean
  local std_time_s=$std

  compute_mean_std "${mem_kb_array[@]}"
  local mean_mem_kb=$mean
  local std_mem_kb=$std

  echo "runtime (s):            $(printf "%.3f" $mean_time_s) ($(printf "%.3f" $std_time_s))"
  echo "max resident size (kb): $(printf "%.0f" $mean_mem_kb) ($(printf "%.0f" $std_mem_kb))"
}

echo "Profiling within-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt")" > "./profiling/results/basic.within.${CURRENT_COMMIT}"

echo "Profiling cross-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt ./test_files/cdr3b_1m_b.txt")" > "./profiling/results/basic.cross.${CURRENT_COMMIT}"
