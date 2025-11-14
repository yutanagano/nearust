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
sem=0
function compute_mean_sem {
  local vals=("$@")
  local len_vals=${#vals[@]}

  local sum=0
  for val in "${vals[@]}"; do
    sum=$(echo "$sum + $val" | bc -l)
  done
  mean=$(echo "$sum / ${#vals[@]}" | bc -l)

  local sum=0
  for val in "${vals[@]}"; do
    sum=$(echo "$sum + ($val - $mean) * ($val - $mean)" | bc -l)
  done
  local std=$(echo "sqrt($sum / ($len_vals - 1))" | bc -l)
  sem=$(echo "$std / sqrt($len_vals)" | bc -l)
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

  compute_mean_sem "${time_s_array[@]}"
  local mean_time_s=$mean
  local sem_time_s=$sem

  compute_mean_sem "${mem_kb_array[@]}"
  local mean_mem_kb=$mean
  local sem_mem_kb=$sem

  echo "runtime (s):"
  echo "  mean: $(printf "%.3f" $mean_time_s)"
  echo "  sem:  $(printf "%.3f" $sem_time_s)"
  echo "max resident size (kb):"
  echo "  mean: $(printf "%.0f" $mean_mem_kb)"
  echo "  sem:  $(printf "%.0f" $sem_mem_kb)"
}

echo "Profiling within-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt")" > "./profiling/results/basic.within.${CURRENT_COMMIT}"

echo "Profiling cross-set symdel..."
echo "$(basic_benchmarking "./target/release/nearust ./test_files/cdr3b_1m_a.txt ./test_files/cdr3b_1m_b.txt")" > "./profiling/results/basic.cross.${CURRENT_COMMIT}"
