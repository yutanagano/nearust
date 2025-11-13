#! /bin/sh
echo "NOTE: make sure you are running this shell script from outside the Python venv or else rustc will complain about some linking issues"

CURRENT_COMMIT=$(git rev-parse --short HEAD)
CURRENT_TIME=$(date -Iseconds)

echo "Building CLI binary..."
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

echo "Building Python package..."
source ./.venv/bin/activate
pip install .[dev]

if [ ! -d "profiling/results" ]; then
  mkdir "profiling/results"
fi

echo "Running perf on CLI..."
perf record -g -o "profiling/results/${CURRENT_COMMIT}_perf_within_${CURRENT_TIME}.data" ./target/release/nearust test_files/cdr3b_1m_a.txt > /dev/null
perf record -g -o "profiling/results/${CURRENT_COMMIT}_perf_cross_${CURRENT_TIME}.data" ./target/release/nearust test_files/cdr3b_1m_a.txt test_files/cdr3b_1m_b.txt > /dev/null

echo "Running memray on Python package..."
memray run -o "/tmp/memray_within_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_within.py
memray run -o "/tmp/memray_cross_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_cross.py
memray run -o "/tmp/memray_memoized_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_memoized.py

memray flamegraph -o "profiling/results/${CURRENT_COMMIT}_memray_within_${CURRENT_TIME}.html" "/tmp/memray_within_${CURRENT_TIME}.bin"
memray flamegraph -o "profiling/results/${CURRENT_COMMIT}_memray_cross_${CURRENT_TIME}.html" "/tmp/memray_cross_${CURRENT_TIME}.bin"
memray flamegraph -o "profiling/results/${CURRENT_COMMIT}_memray_memoized_${CURRENT_TIME}.html" "/tmp/memray_memoized_${CURRENT_TIME}.bin"
